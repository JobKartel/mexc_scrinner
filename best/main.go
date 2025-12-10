// main.go
package main

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"math"
	"math/rand"
	mexc "mexc_init/proto"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"gopkg.in/telebot.v4"
)

var json = jsoniter.ConfigFastest

// --- Конфиг ---
const (
	wsPrimaryURL      = "wss://wbs-api.mexc.com/ws" // основной
	wsFallbackURL     = "ws://wbs-api.mexc.com/ws"  // запасной
	exchangeInfoURL   = "https://api.mexc.com/api/v3/exchangeInfo"
	tickerPriceURL    = "https://api.mexc.com/api/v3/ticker/price"
	defaultTGToken    = "7638339315:AAHuTTvh9Rtzu_XieCOlin8RVcbUACe2ZRY" // можно переопределить через ENV TELEGRAM_BOT_TOKEN
	subscribeBatchMax = 30
	readTimeout       = 90 * time.Second
	jsonPingEvery     = 15 * time.Second
	wsPingEvery       = 25 * time.Second
	processEvery      = 5 * time.Second
	pongTimeout       = 60 * time.Second
)

var (
	isProcessingStarted bool
	processingMutex     sync.Mutex
)

// --- Модели данных ---
type TradeData struct {
	Price     float64
	Quantity  float64
	TradeTime int64
	Side      int // 1 buy, 2 sell
}

type ProcessingEvent struct {
	trades          TradesData
	users           UsersData
	connections     map[*WebSocketConnection]struct{}
	connMutex       sync.RWMutex
	invalidSymbols  map[string]struct{}
	invalidSymMutex sync.RWMutex
}

type WebSocketConnection struct {
	Conn     *websocket.Conn
	Params   []string
	LastPong time.Time
	IsActive bool
	Mutex    sync.Mutex
}

type TradesData struct {
	sync.RWMutex
	tradeData map[string][]TradeData
}

type UsersData struct {
	sync.RWMutex
	userSettings map[int64]*UserSettings
}

type UserSettings struct {
	MinVolume            float64
	PriceChangeThreshold float64
	IntervalDuration     time.Duration
	LastProcessedTime    map[string]time.Time
}

type PairMetrics struct {
	Symbol             string
	PriceChangePercent float64
	TotalVolume        float64
	OpenPrice          float64
	ClosePrice         float64
}

type TelegramMessage struct {
	ChatID  int64
	Text    string
	Retries int
}

// Подтверждение подписки
type subAck struct {
	ID   uint32 `json:"id"`
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// --- Инициализация ---
func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.InfoLevel)
	rand.Seed(time.Now().UnixNano())
}

func NewProcessingEvent() *ProcessingEvent {
	return &ProcessingEvent{
		trades: TradesData{
			tradeData: make(map[string][]TradeData),
		},
		users: UsersData{
			userSettings: make(map[int64]*UserSettings),
		},
		connections:    make(map[*WebSocketConnection]struct{}),
		invalidSymbols: make(map[string]struct{}),
	}
}

// --- Метрики WS: пары и соединения ---
func (p *ProcessingEvent) logWsStats() {
	// Снимок активных соединений
	p.connMutex.RLock()
	conns := make([]*WebSocketConnection, 0, len(p.connections))
	for c := range p.connections {
		conns = append(conns, c)
	}
	p.connMutex.RUnlock()

	// Уникальные пары из подписок каждого соединения
	uniq := make(map[string]struct{}, 4096)
	for _, c := range conns {
		for _, ch := range c.Params {
			if s := symbolFromChannel(ch); s != "" {
				uniq[s] = struct{}{}
			}
		}
	}

	logrus.WithFields(logrus.Fields{
		"pairs":       len(uniq),
		"connections": len(conns),
	}).Info("WS stats")
}

// --- main ---
func main() {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	if token == "" {
		token = defaultTGToken
	}

	pref := telebot.Settings{
		Token:  token,
		Poller: &telebot.LongPoller{Timeout: 5 * time.Second},
	}
	bot, err := telebot.NewBot(pref)
	if err != nil {
		logrus.Fatalf("Failed to create Telegram bot: %v", err)
	}

	procEvent := NewProcessingEvent()

	// Команды
	bot.Handle("/start", func(c telebot.Context) error {
		chatID := c.Chat().ID

		procEvent.users.Lock()
		defer procEvent.users.Unlock()

		if _, exists := procEvent.users.userSettings[chatID]; exists {
			return c.Send("Вы уже зарегистрированы для уведомлений.")
		}
		procEvent.users.userSettings[chatID] = &UserSettings{
			MinVolume:            1000.0,
			PriceChangeThreshold: 2.0,
			IntervalDuration:     5 * time.Second,
			LastProcessedTime:    make(map[string]time.Time),
		}

		processingMutex.Lock()
		if !isProcessingStarted {
			isProcessingStarted = true
			go procEvent.startWebSocketAndSendNotifications(bot)
		}
		processingMutex.Unlock()

		return c.Send("Готово! Вы подписаны на уведомления. Настройте параметры через /set")
	})

	bot.Handle("/set", func(c telebot.Context) error {
		args := c.Args()
		if len(args) != 3 {
			return c.Send("❌ Требуется 3 параметра: /set [объем$] [процент] [интервал]\nНапример: /set 3000 2 5s")
		}
		chatID := c.Chat().ID

		procEvent.users.Lock()
		defer procEvent.users.Unlock()

		settings, ok := procEvent.users.userSettings[chatID]
		if !ok {
			return c.Send("❌ Сначала выполните /start")
		}
		volume, err := strconv.ParseFloat(args[0], 64)
		if err != nil || volume <= 0 {
			return c.Send("❌ Неверный объем. Пример: 3000")
		}
		percent, err := strconv.ParseFloat(args[1], 64)
		if err != nil || percent <= 0 {
			return c.Send("❌ Неверный процент. Пример: 2")
		}
		interval, err := time.ParseDuration(args[2])
		if err != nil || interval < time.Second {
			return c.Send("❌ Неверный интервал. Используйте s/m/h. Пример: 5s")
		}

		settings.MinVolume = volume
		settings.PriceChangeThreshold = percent
		settings.IntervalDuration = interval

		logrus.Infof("User %d settings updated: Volume=%.2f, Percent=%.2f%%, Interval=%s",
			chatID, volume, percent, interval)

		return c.Send(fmt.Sprintf("✅ Установлено:\n• Мин. объём: %.2f $\n• Изм. цены: %.2f%%\n• Интервал: %s",
			volume, percent, interval))
	})

	logrus.Info("Starting Telegram bot")
	bot.Start()
}

// --- Ping JSON ---
func sendJSONPing(conn *websocket.Conn) error {
	ping := map[string]any{
		"method": "PING",
		"id":     rand.Uint32(),
	}
	return conn.WriteJSON(ping)
}

// --- Главный цикл: получение пар, коннекты, подписки, обработка ---
func (p *ProcessingEvent) startWebSocketAndSendNotifications(bot *telebot.Bot) {
	// Мониторинг соединений и авто-переподключение
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			p.logWsStats()
			p.checkAndReconnect(bot)
		}
	}()

	for {
		logrus.Info("Starting WebSocket connections")

		pairs, err := p.getStablecoinTradingPairs()
		if err != nil {
			logrus.Errorf("Failed to get trading pairs: %v. Retry in 10s", err)
			time.Sleep(10 * time.Second)
			continue
		}

		// формируем канальные строки
		params := make([]string, 0, len(pairs))
		for _, pair := range pairs {
			params = append(params, fmt.Sprintf("spot@public.aggre.deals.v3.api.pb@100ms@%s", pair))
		}

		// батчи по 30
		var batches [][]string
		for i := 0; i < len(params); i += subscribeBatchMax {
			j := i + subscribeBatchMax
			if j > len(params) {
				j = len(params)
			}
			batches = append(batches, params[i:j])
		}

		// чтобы не плодить слишком много коннектов
		maxConns := 120
		if len(batches) > maxConns {
			batches = batches[:maxConns]
		}

		// ограничим параллелизм подключения
		sem := make(chan struct{}, len(batches))
		var wg sync.WaitGroup
		for _, batch := range batches {
			wg.Add(1)
			sem <- struct{}{}
			go func(batch []string) {
				defer wg.Done()
				defer func() { <-sem }()
				time.Sleep(time.Duration(800+rand.Intn(1200)) * time.Millisecond)
				p.startWebSocketConnectionWithReconnect(bot, batch)
			}(batch)
		}
		wg.Wait()

		logrus.Error("All WebSocket connections closed. Restarting in 10s")
		time.Sleep(10 * time.Second)
	}
}

func (p *ProcessingEvent) checkAndReconnect(bot *telebot.Bot) {
	p.connMutex.Lock()
	conns := make([]*WebSocketConnection, 0, len(p.connections))
	for c := range p.connections {
		conns = append(conns, c)
	}
	p.connMutex.Unlock()

	for _, wsConn := range conns {
		wsConn.Mutex.Lock()
		inactive := !wsConn.IsActive || time.Since(wsConn.LastPong) > pongTimeout
		wsConn.Mutex.Unlock()

		if inactive {
			logrus.Warnf("Connection for params %v is inactive. Reconnecting...", wsConn.Params)
			if wsConn.Conn != nil {
				_ = wsConn.Conn.Close()
			}
			p.connMutex.Lock()
			delete(p.connections, wsConn)
			p.connMutex.Unlock()
			go p.startWebSocketConnectionWithReconnect(bot, wsConn.Params)
		}
	}
}

func (p *ProcessingEvent) startWebSocketConnectionWithReconnect(bot *telebot.Bot, params []string) {
	baseDelay := 2 * time.Second
	maxDelay := 5 * time.Minute
	delay := baseDelay

	for {
		logrus.Infof("Attempting to connect WebSocket for params: %v", params)
		wsConn := &WebSocketConnection{Params: params, LastPong: time.Now()}

		err := p.startWebSocketConnection(bot, wsConn)
		if err == nil {
			delay = baseDelay
			continue
		}
		if strings.Contains(err.Error(), "close 1005") && delay < 30*time.Second {
			delay = 30 * time.Second
		}
		logrus.Errorf("WebSocket connection failed: %v. Retrying in %v", err, delay)
		time.Sleep(delay)
		// экспоненциальный backoff
		next := time.Duration(float64(delay) * 1.5)
		if next > maxDelay {
			next = maxDelay
		}
		delay = next
	}
}

func (p *ProcessingEvent) startWebSocketConnection(bot *telebot.Bot, wsConn *WebSocketConnection) error {
	dialer := *websocket.DefaultDialer
	dialer.Proxy = http.ProxyFromEnvironment
	dialer.HandshakeTimeout = 12 * time.Second
	dialer.EnableCompression = true

	endpoints := []string{wsPrimaryURL, wsFallbackURL}
	var conn *websocket.Conn
	var err error
	for _, ep := range endpoints {
		conn, _, err = dialer.Dial(ep, nil)
		if err == nil {
			logrus.WithField("url", ep).Info("WebSocket connected")
			break
		}
		logrus.WithField("url", ep).Errorf("Dial error: %v", err)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}

	// Настройки сокета
	conn.SetReadLimit(32 << 20) // 32 МБ на всякий случай
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetCloseHandler(func(code int, text string) error {
		logrus.WithFields(logrus.Fields{"code": code, "text": text}).Warn("CloseHandler")
		return nil
	})
	wsConn.Conn = conn
	wsConn.IsActive = true
	wsConn.LastPong = time.Now()

	// pong/ping
	conn.SetPongHandler(func(appData string) error {
		wsConn.Mutex.Lock()
		wsConn.LastPong = time.Now()
		wsConn.Mutex.Unlock()
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})
	conn.SetPingHandler(func(appData string) error {
		wsConn.Mutex.Lock()
		wsConn.LastPong = time.Now()
		wsConn.Mutex.Unlock()
		_ = conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})

	// Подписка с ожиданием ACK (поштучно)
	okCh, ackErr := p.subscribeWithAcks(conn, wsConn.Params, 4*time.Second)
	if ackErr != nil || len(okCh) == 0 {
		_ = conn.Close()
		return fmt.Errorf("subscription failed: %w", ackErr)
	}
	wsConn.Params = okCh
	logrus.WithField("subs_ok", len(okCh)).Info("Subscriptions acknowledged")

	// Зарегистрировать соединение
	p.connMutex.Lock()
	p.connections[wsConn] = struct{}{}
	p.connMutex.Unlock()

	// Тикеры
	dataTicker := time.NewTicker(processEvery)
	jsonPingTicker := time.NewTicker(jsonPingEvery)
	wsPingTicker := time.NewTicker(wsPingEvery)
	done := make(chan error, 1)

	defer func() {
		dataTicker.Stop()
		jsonPingTicker.Stop()
		wsPingTicker.Stop()
		p.connMutex.Lock()
		wsConn.IsActive = false
		delete(p.connections, wsConn)
		p.connMutex.Unlock()
		_ = conn.Close()
	}()

	// Reader
	go p.readWebSocketMessages(context.Background(), wsConn, done)

	for {
		select {
		case <-dataTicker.C:
			p.processTradeData(bot, wsConn.Params)

		case <-jsonPingTicker.C:
			if err := sendJSONPing(conn); err != nil {
				return fmt.Errorf("failed to send JSON PING: %w", err)
			}

		case <-wsPingTicker.C:
			_ = conn.WriteControl(websocket.PingMessage, []byte(strconv.FormatInt(time.Now().Unix(), 10)), time.Now().Add(5*time.Second))

		case err := <-done:
			if err == nil {
				return fmt.Errorf("connection closed")
			}
			return fmt.Errorf("reader exited: %w", err)
		}
	}
}

// --- Чтение сообщений сокета ---
func (p *ProcessingEvent) readWebSocketMessages(ctx context.Context, wsConn *WebSocketConnection, done chan<- error) {
	const maxTradesPerSymbol = 2000

	defer func() {
		select {
		case done <- nil:
		default:
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			mt, payload, err := wsConn.Conn.ReadMessage()
			if err != nil {
				select {
				case done <- err:
				default:
				}
				return
			}

			_ = wsConn.Conn.SetReadDeadline(time.Now().Add(readTimeout))

			switch mt {
			case websocket.BinaryMessage:
				var wrapper mexc.PushDataV3ApiWrapper
				if err = proto.Unmarshal(payload, &wrapper); err != nil {
					logrus.Errorf("protobuf unmarshal failed: %v", err)
					continue
				}
				symbol := wrapper.GetSymbol()
				if symbol == "" {
					ch := wrapper.GetChannel()
					if ch != "" {
						if s := symbolFromChannel(ch); s != "" {
							symbol = s
						}
					}
				}
				if symbol == "" {
					continue
				}

				// aggregated deals
				if ad := wrapper.GetPublicAggreDeals(); ad != nil {
					items := ad.GetDeals()
					if len(items) == 0 {
						continue
					}
					trades := make([]TradeData, 0, len(items))
					for _, it := range items {
						price, err1 := strconv.ParseFloat(it.GetPrice(), 64)
						qty, err2 := strconv.ParseFloat(it.GetQuantity(), 64)
						if err1 != nil || err2 != nil {
							continue
						}
						trades = append(trades, TradeData{
							Price:     price,
							Quantity:  qty,
							TradeTime: it.GetTime(),
							Side:      int(it.GetTradeType()),
						})
					}
					if len(trades) == 0 {
						continue
					}
					p.trades.Lock()
					cur := p.trades.tradeData[symbol]
					need := len(cur) + len(trades) - maxTradesPerSymbol
					if need > 0 {
						if need < len(cur) {
							cur = cur[need:]
						} else {
							cur = cur[:0]
						}
					}
					p.trades.tradeData[symbol] = append(cur, trades...)
					p.trades.Unlock()
					continue
				}

				// plain deals
				if d := wrapper.GetPublicDeals(); d != nil {
					items := d.GetDeals()
					if len(items) == 0 {
						continue
					}
					trades := make([]TradeData, 0, len(items))
					for _, it := range items {
						price, err1 := strconv.ParseFloat(it.GetPrice(), 64)
						qty, err2 := strconv.ParseFloat(it.GetQuantity(), 64)
						if err1 != nil || err2 != nil {
							continue
						}
						trades = append(trades, TradeData{
							Price:     price,
							Quantity:  qty,
							TradeTime: it.GetTime(),
							Side:      int(it.GetTradeType()),
						})
					}
					if len(trades) == 0 {
						continue
					}
					p.trades.Lock()
					cur := p.trades.tradeData[symbol]
					need := len(cur) + len(trades) - maxTradesPerSymbol
					if need > 0 {
						if need < len(cur) {
							cur = cur[need:]
						} else {
							cur = cur[:0]
						}
					}
					p.trades.tradeData[symbol] = append(cur, trades...)
					p.trades.Unlock()
					continue
				}

			case websocket.TextMessage:
				s := string(payload)
				// JSON "PONG"
				if strings.Contains(s, `"PONG"`) {
					wsConn.Mutex.Lock()
					wsConn.LastPong = time.Now()
					wsConn.Mutex.Unlock()
					continue
				}
				// ACK логируем для отладки
				var ack subAck
				if err := json.Unmarshal(payload, &ack); err == nil {
					logrus.WithFields(logrus.Fields{"code": ack.Code, "msg": ack.Msg}).Debug("ACK")
				}
			}
		}
	}
}

// --- Подписка с ожиданием ACK по каждому каналу ---
func (p *ProcessingEvent) subscribeWithAcks(conn *websocket.Conn, channels []string, perAckTimeout time.Duration) ([]string, error) {
	okChannels := make([]string, 0, len(channels))

	// отфильтруем заранее помеченные как плохие
	filtered := make([]string, 0, len(channels))
	p.invalidSymMutex.RLock()
	for _, ch := range channels {
		if sym := symbolFromChannel(ch); sym != "" {
			if _, bad := p.invalidSymbols[sym]; bad {
				continue
			}
			filtered = append(filtered, ch)
		}
	}
	p.invalidSymMutex.RUnlock()

	for _, ch := range filtered {
		req := map[string]any{
			"method": "SUBSCRIPTION",
			"id":     rand.Uint32(),
			"params": []string{ch},
		}
		if err := conn.WriteJSON(req); err != nil {
			return okChannels, fmt.Errorf("write sub for %s: %w", ch, err)
		}

		deadline := time.Now().Add(perAckTimeout)
		_ = conn.SetReadDeadline(deadline)

		ackOK := false

	ackLoop:
		for {
			// общий выход по времени
			if time.Now().After(deadline) {
				break
			}

			mt, payload, rerr := conn.ReadMessage()
			if rerr != nil {
				// если ACK уже получен, не считаем это ошибкой рукопожатия
				if ackOK {
					break
				}
				return okChannels, fmt.Errorf("read ack for %s: %w", ch, rerr)
			}

			switch mt {
			case websocket.TextMessage:
				var ack subAck
				if json.Unmarshal(payload, &ack) == nil {
					// положительный ACK ИМЕННО для нашего канала
					if ack.Msg == ch && ack.Code == 0 {
						ackOK = true
						logrus.WithField("channel", ch).Info("ACK ok")
						break ackLoop // <<< ВАЖНО: выходим из цикла ожидания ACK
					}
					// отрицательный ACK — помечаем символ и не ждём дальше
					if ack.Msg == ch && ack.Code != 0 {
						logrus.WithFields(logrus.Fields{
							"channel": ch, "code": ack.Code, "msg": ack.Msg,
						}).Warn("ACK error for channel")
						if sym := symbolFromChannel(ch); sym != "" {
							p.invalidSymMutex.Lock()
							p.invalidSymbols[sym] = struct{}{}
							p.invalidSymMutex.Unlock()
						}
						ackOK = false
						break ackLoop // <<< тоже выходим
					}
					// чужой ACK — игнорируем и продолжаем ждать наш
				}
			case websocket.BinaryMessage:
				// бинарку во время рукопожатия игнорируем
			}
		}

		// восстановим стандартный дедлайн чтения
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))

		if ackOK {
			okChannels = append(okChannels, ch)
		} else {
			logrus.WithField("channel", ch).Warn("subscription not acknowledged")
		}
	}

	if len(okChannels) == 0 {
		return okChannels, fmt.Errorf("no channels acknowledged")
	}
	return okChannels, nil
}

// --- Обработка и уведомления ---
func (p *ProcessingEvent) processTradeData(bot *telebot.Bot, params []string) {
	p.trades.RLock()
	if len(p.trades.tradeData) == 0 {
		p.trades.RUnlock()
		return
	}

	tradeDataCopy := make(map[string][]TradeData, len(params))
	for _, param := range params {
		if sym := symbolFromChannel(param); sym != "" {
			if trades, ok := p.trades.tradeData[sym]; ok && len(trades) > 0 {
				cp := make([]TradeData, len(trades))
				copy(cp, trades)
				tradeDataCopy[sym] = cp
			}
		}
	}
	p.trades.RUnlock()

	var wg sync.WaitGroup
	for pair, trades := range tradeDataCopy {
		wg.Add(1)
		go func(pair string, trades []TradeData) {
			defer wg.Done()
			if metrics := p.computeMetrics(pair, trades); metrics != nil {
				p.notifyUsers(metrics, bot)
			}
		}(pair, trades)
	}
	wg.Wait()
}

func (p *ProcessingEvent) notifyUsers(metrics *PairMetrics, bot *telebot.Bot) {
	p.users.Lock()
	defer p.users.Unlock()

	messageQueue := make(chan TelegramMessage, 128)
	go func() {
		for msg := range messageQueue {
			for i := 0; i <= msg.Retries; i++ {
				_, err := bot.Send(telebot.ChatID(msg.ChatID), msg.Text)
				if err == nil {
					logrus.Infof("Message sent to user %d for pair %s", msg.ChatID, metrics.Symbol)
					break
				}
				logrus.Errorf("Failed to send message to user %d: %v. Attempt %d", msg.ChatID, err, i+1)
				time.Sleep(time.Second * time.Duration(i+1))
			}
		}
	}()

	now := time.Now()
	for chatID, settings := range p.users.userSettings {
		lastProcessed := settings.LastProcessedTime[metrics.Symbol]
		if now.Sub(lastProcessed) >= settings.IntervalDuration {
			if metrics.TotalVolume >= settings.MinVolume && math.Abs(metrics.PriceChangePercent) >= settings.PriceChangeThreshold {
				settings.LastProcessedTime[metrics.Symbol] = now
				priceEmoji := getPriceEmoji(metrics.PriceChangePercent)
				volumeEmoji := getVolumeEmoji(metrics.TotalVolume)
				msg := fmt.Sprintf("%s\n%.2f%% %s\nОбъём: %.2f$ %s\n",
					formatPair(metrics.Symbol),
					metrics.PriceChangePercent,
					priceEmoji,
					metrics.TotalVolume,
					volumeEmoji,
				)
				messageQueue <- TelegramMessage{ChatID: chatID, Text: msg, Retries: 3}
			}
		}
	}
	close(messageQueue)
}

func (p *ProcessingEvent) computeMetrics(symbol string, trades []TradeData) *PairMetrics {
	if len(trades) == 0 {
		return nil
	}
	const windowMs int64 = 5000
	now := time.Now().UnixNano() / int64(time.Millisecond)
	from := now - windowMs

	filtered := make([]TradeData, 0, len(trades))
	for _, tr := range trades {
		if tr.TradeTime >= from {
			filtered = append(filtered, tr)
		}
	}
	if len(filtered) == 0 {
		return nil
	}

	open := filtered[0].Price
	closep := filtered[len(filtered)-1].Price
	hi, lo := open, open
	vol := 0.0
	for _, tr := range filtered {
		if tr.Price > hi {
			hi = tr.Price
		}
		if tr.Price < lo {
			lo = tr.Price
		}
		vol += tr.Price * tr.Quantity
	}
	change := 0.0
	if lo > 0 {
		change = (hi - lo) / lo * 100
	}
	return &PairMetrics{
		Symbol:             symbol,
		PriceChangePercent: change,
		TotalVolume:        vol,
		OpenPrice:          open,
		ClosePrice:         closep,
	}
}

// --- Получение списка валидных пар ---
type exSymbol struct {
	Symbol               string `json:"symbol"`
	Status               string `json:"status"` // TRADING / ENABLED / AVAILABLE / ...
	BaseAsset            string `json:"baseAsset"`
	QuoteAsset           string `json:"quoteAsset"`
	IsSpotTradingAllowed *bool  `json:"isSpotTradingAllowed,omitempty"`
}
type exInfo struct {
	Symbols []exSymbol `json:"symbols"`
}

func (p *ProcessingEvent) getStablecoinTradingPairs() ([]string, error) {
	const maxRetries = 5
	baseDelay := time.Second
	client := &http.Client{Timeout: 12 * time.Second}

	// критерий годности
	isGood := func(s exSymbol) bool {
		qa := strings.ToUpper(s.QuoteAsset)
		if qa != "USDT" && qa != "USDC" {
			return false
		}
		st := strings.ToUpper(s.Status)
		if st != "" && st != "TRADING" && st != "ENABLED" && st != "AVAILABLE" {
			return false
		}
		if s.IsSpotTradingAllowed != nil && !*s.IsSpotTradingAllowed {
			return false
		}
		if len(s.Symbol) < 6 {
			return false
		}
		return true
	}

	// 1) exchangeInfo
	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err := client.Get(exchangeInfoURL)
		if err != nil {
			if attempt < maxRetries {
				time.Sleep(baseDelay * time.Duration(attempt))
			}
			continue
		}
		var ei exInfo
		func() {
			defer resp.Body.Close()
			_ = json.NewDecoder(resp.Body).Decode(&ei)
		}()
		if len(ei.Symbols) > 0 {
			seen := make(map[string]struct{}, len(ei.Symbols))
			out := make([]string, 0, len(ei.Symbols))
			for _, s := range ei.Symbols {
				if isGood(s) {
					u := strings.ToUpper(s.Symbol)
					if _, ok := seen[u]; !ok {
						seen[u] = struct{}{}
						out = append(out, u)
					}
				}
			}
			if len(out) > 0 {
				return out, nil
			}
		}
		if attempt < maxRetries {
			time.Sleep(baseDelay * time.Duration(attempt))
		}
	}

	// 2) Фоллбэк на /ticker/price + фильтр по суффиксу
	type priceItem struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}
	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err := client.Get(tickerPriceURL)
		if err != nil {
			if attempt < maxRetries {
				time.Sleep(baseDelay * time.Duration(attempt))
			}
			continue
		}
		var arr []priceItem
		func() {
			defer resp.Body.Close()
			_ = json.NewDecoder(resp.Body).Decode(&arr)
		}()
		if len(arr) > 0 {
			seen := make(map[string]struct{}, len(arr))
			out := make([]string, 0, len(arr))
			for _, it := range arr {
				u := strings.ToUpper(it.Symbol)
				if strings.HasSuffix(u, "USDT") || strings.HasSuffix(u, "USDC") {
					if _, ok := seen[u]; !ok {
						seen[u] = struct{}{}
						out = append(out, u)
					}
				}
			}
			if len(out) > 0 {
				return out, nil
			}
		}
		if attempt < maxRetries {
			time.Sleep(baseDelay * time.Duration(attempt))
		}
	}

	return nil, fmt.Errorf("failed to fetch trading pairs")
}

// --- Разное утилитарное ---
func formatPair(text string) string {
	if strings.Contains(text, "_") {
		return strings.Replace(text, "_", "/", 1)
	}
	if len(text) > 4 {
		// BTCUSDT -> BTC/USDT (для красоты)
		if strings.HasSuffix(text, "USDT") {
			return text[:len(text)-4] + "/USDT"
		}
		if strings.HasSuffix(text, "USDC") {
			return text[:len(text)-4] + "/USDC"
		}
	}
	return text
}

func getPriceEmoji(p float64) string {
	abs := math.Abs(p)
	switch {
	case abs >= 30:
		if p < 0 {
			return "🔴 🔴 🔴 🔴 🔴"
		}
		return "🟢 🟢 🟢 🟢 🟢"
	case abs >= 20:
		if p < 0 {
			return "🔴 🔴 🔴"
		}
		return "🟢 🟢 🟢"
	case abs >= 10:
		if p < 0 {
			return "🔴 🔴"
		}
		return "🟢 🟢"
	case abs >= 2:
		if p < 0 {
			return "🔴"
		}
		return "🟢"
	default:
		return ""
	}
}

func getVolumeEmoji(v float64) string {
	switch {
	case v >= 50000:
		return "🔥 🔥 🔥 🔥 🔥"
	case v >= 40000:
		return "🔥 🔥 🔥"
	case v >= 30000:
		return "🔥 🔥"
	case v >= 20000:
		return "🔥"
	case v >= 10000:
		return "👁"
	default:
		return ""
	}
}

func symbolFromChannel(ch string) string {
	// формат: spot@public.aggre.deals.v3.api.pb@100ms@SYMBOL
	if ch == "" {
		return ""
	}
	parts := strings.Split(ch, "@")
	if len(parts) == 0 {
		return ""
	}
	return strings.ToUpper(parts[len(parts)-1])
}
