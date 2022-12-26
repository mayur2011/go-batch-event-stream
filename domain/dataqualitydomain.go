package domain

type DQBatchProcess struct {
	ID             string `bson:"_id"`
	Name           string `bson:"jobName"`
	BatchType      string `bson:"jobType"`
	NextRunTime    string `bson:"nextRunTime"`
	CollectionName string `bson:"sourceCollection"`
	MessageQueue   string `bson:"queue"`
}

type Timeliness struct {
	OrderId    string `bson:"orderId"`
	Segment    string `bson:"segment"`
	Country    string `bson:"country"`
	Status     string `bson:"status"`
	RecordDate string `bson:"recordDate"`
}
