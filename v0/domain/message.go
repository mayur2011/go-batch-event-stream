package domain

type Message struct {
	SeqNum    int    `json:"seqNumber"`
	MsgType   string `json:"msgType"`
	OrderId   string `json:"orderId"`
	Country   string `json:"country"`
	EventTime string `json:"EventTimestamp"`
}
