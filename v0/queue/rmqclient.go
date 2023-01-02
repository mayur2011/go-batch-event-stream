package queue

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"go-batch-event-stream/config"
	"io/ioutil"
	"math"
	"net/url"
	"time"

	log "github.com/sirupsen/logrus"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	Conn *amqp.Connection
}

func tlsClientConfig(env string) *tls.Config {
	certFile := env + "-rmq_certificate.cer"
	certFilePath := "./cert/" + certFile

	caCert, err := ioutil.ReadFile(certFilePath)
	if err != nil {
		log.Println("Error while reading the certificate file..", err)
	}
	cfg := new(tls.Config)
	cfg.RootCAs = x509.NewCertPool()
	cfg.RootCAs.AppendCertsFromPEM(caCert)

	return cfg
}

func NewAMQPConnectionWithTLS(env string, conf config.RabbitMQType) (Client, error) {
	var counts int64
	var backOff = 1 * time.Second
	var connection *amqp.Connection

	scheme := url.QueryEscape(conf.Scheme)
	username := url.QueryEscape(conf.Username)
	password := url.QueryEscape(conf.Password)
	hostname := url.QueryEscape(conf.Hostname)
	portnum := url.QueryEscape(fmt.Sprintf("%d", conf.Port))
	vhost := conf.VHost

	encodedURI := scheme + "://" + username + ":" + password + "@" + hostname + ":" + portnum + "/" + vhost

	// don't continue until rabbitmq is ready
	for {
		c, err := amqp.DialTLS(encodedURI, tlsClientConfig(env))
		if err != nil {
			log.Errorln("RabbitMQ not yet ready...")
			counts++
		} else {
			connection = c
			break
		}
		if counts > 3 {
			fmt.Println(err)
			return Client{}, err
		}

		backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Errorln("Backing off...")
		time.Sleep(backOff)
		continue
	}
	log.Info("RABBIT-MQ CONNECTION OPEN..!")
	return Client{Conn: connection}, nil
}

func (mqc *Client) CloseAMQPConnection() (bool, error) {
	if err := mqc.Conn.Close(); err != nil {
		return false, err
	}
	log.Info("RABBIT-MQ CONNECTION CLOSED..!")
	return true, nil
}
