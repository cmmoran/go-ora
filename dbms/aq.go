package dbms

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"

	go_ora "github.com/sijms/go-ora/v2"
)

var ErrQueueNotMultiConsumer = errors.New("queue is not multi-consumer")

type AQ struct {
	conn          *sql.DB
	multiConsumer atomic.Bool
	Name          string        `db:"QUEUE_NAME"`
	TableName     string        `db:"TB_NAME"`
	TypeName      string        `db:"TYPE_NAME"`
	MultiConsumer go_ora.PLBool `db:"MULTI_CONSUMER"`
	MaxRetry      int64         `db:"MAX_RETRY"`
	RetryDelay    int64         `db:"RETRY_DELAY"`
	RetentionTime int64         `db:"RETENTION_TIME"`
	Comment       string        `db:"COMMENT"`
	Owner         string
}

// NewAQ creates a new instance of AQ with the provided connection, queue name, and type name. The table name is generated with a "_TB" suffix.
// To make this QA multi-consumer, set AQ.MultiConsumer on the resulting instance.
// Doing so will enable the AQ.Subscribe, AQ.Unsubscribe, AQ.DequeueForSubscriber functions.
func NewAQ(conn *sql.DB, name, typeName string, multiConsumer ...bool) *AQ {
	mc := len(multiConsumer) > 0 && multiConsumer[0]
	output := &AQ{
		conn:          conn,
		Name:          name,
		TableName:     name + "_TB",
		TypeName:      typeName,
		RetentionTime: -1,
		Comment:       name,
		MultiConsumer: go_ora.PLBool(mc),
	}
	return output
}

func (aq *AQ) validate() error {
	if aq.conn == nil {
		return errors.New("no connection defined for AQ type")
	}
	if len(aq.Name) == 0 {
		return errors.New("queue name cannot be null")
	}
	if len(aq.TypeName) == 0 {
		return errors.New("type name cannot be null")
	}
	return nil
}

func (aq *AQ) Create() error {
	err := aq.validate()
	if err != nil {
		return err
	}
	sqlText := `BEGIN
	DBMS_AQADM.CREATE_QUEUE_TABLE (
		queue_table => :TB_NAME,
		queue_payload_type => :TYPE_NAME,
		multiple_consumers => :MULTI_CONSUMER
	);

	DBMS_AQADM.CREATE_QUEUE (
		:QUEUE_NAME, :TB_NAME, DBMS_AQADM.NORMAL_QUEUE, 
		:MAX_RETRY, :RETRY_DELAY, :RETENTION_TIME, 
		FALSE, :COMMENT);
END;`
	_, err = aq.conn.Exec(sqlText, aq)
	if err == nil {
		aq.multiConsumer.Store(bool(aq.MultiConsumer))
	}
	return err
}

func (aq *AQ) Drop() error {
	err := aq.validate()
	if err != nil {
		return err
	}
	sqlText := `BEGIN
	DBMS_AQADM.DROP_QUEUE(:QUEUE_NAME, FALSE);
	DBMS_AQADM.DROP_QUEUE_TABLE(:TB_NAME);
END;`
	_, err = aq.conn.Exec(sqlText, aq) // sql.Named("QUEUE_NAME", aq.Name),
	// sql.Named("TABLE_NAME", aq.TableName))
	return err
}

// Start enable both enqueue and dequeue
func (aq *AQ) Start(enqueue, dequeue bool) error {
	err := aq.validate()
	if err != nil {
		return err
	}
	_, err = aq.conn.Exec(`BEGIN
dbms_aqadm.start_queue (queue_name => :QUEUE_NAME, 
                       enqueue => :ENQUEUE , 
                       dequeue => :DEQUEUE); 
 END;`, aq.Name, go_ora.PLBool(enqueue), go_ora.PLBool(dequeue))
	return err
}

// Stop disable both enqueue and dequeue
func (aq *AQ) Stop(enqueue, dequeue bool) error {
	err := aq.validate()
	if err != nil {
		return err
	}
	_, err = aq.conn.Exec(`BEGIN
dbms_aqadm.stop_queue(queue_name => :QUEUE_NAME, 
                       enqueue => :ENQUEUE , 
                       dequeue => :DEQUEUE); 
 END;`, aq.Name, go_ora.PLBool(enqueue), go_ora.PLBool(dequeue))
	return err
}

// Subscribe adds a new subscriber (consumer) on a multi-consumer queue.
// Your queue must have been created with multiple_consumers => TRUE.
func (aq *AQ) Subscribe(subscriberName string) error {
	if err := aq.validate(); err != nil {
		return err
	}
	if !aq.multiConsumer.Load() {
		return errors.New("queue is not multi-consumer")
	}

	const plsql = `
BEGIN
  DBMS_AQADM.ADD_SUBSCRIBER(
    queue_name      => :QUEUE_NAME,
    subscriber      => SYS.AQ$_AGENT(name => :SUBSCRIBER, address => NULL, protocol => NULL),
    rule            => NULL,
    transformation  => NULL,
    queue_to_queue  => FALSE,
    delivery_mode   => DBMS_AQADM.PERSISTENT
  );
  COMMIT;
END;`

	_, err := aq.conn.Exec(
		plsql,
		sql.Named("QUEUE_NAME", aq.Name),
		sql.Named("SUBSCRIBER", subscriberName),
	)
	return err
}

// Unsubscribe unregisters a subscriber from the queue.
func (aq *AQ) Unsubscribe(subscriberName string) error {
	if err := aq.validate(); err != nil {
		return err
	}
	if !aq.multiConsumer.Load() {
		return errors.New("queue is not multi-consumer")
	}
	const plsql = `
	BEGIN
	DBMS_AQADM.REMOVE_SUBSCRIBER(
		queue_name => :QUEUE_NAME,
		subscriber => SYS.AQ$_AGENT(:SUBSCRIBER, NULL, NULL)
);
	COMMIT;
	END;`
	_, err := aq.conn.Exec(
		plsql,
		sql.Named("QUEUE_NAME", aq.Name),
		sql.Named("SUBSCRIBER", subscriberName),
	)
	return err
}

// DequeueForSubscriber pulls the next message for a given subscriber.
// It sets deque_options.CONSUMER_NAME so each subscriber sees its own copy.
func (aq *AQ) DequeueForSubscriber(subscriberName string, message driver.Value, messageSize int) ([]byte, error) {
	if err := aq.validate(); err != nil {
		return nil, err
	}
	if !aq.multiConsumer.Load() {
		return nil, ErrQueueNotMultiConsumer
	}

	const plsql = `
DECLARE
  dequeue_options    DBMS_AQ.DEQUEUE_OPTIONS_T;
  message_properties DBMS_AQ.MESSAGE_PROPERTIES_T;
BEGIN
  dequeue_options.VISIBILITY    := DBMS_AQ.IMMEDIATE;
  dequeue_options.CONSUMER_NAME := :SUBSCRIBER;
  DBMS_AQ.DEQUEUE(
    queue_name         => :QUEUE_NAME,
    dequeue_options    => dequeue_options,
    message_properties => message_properties,
    payload            => :MSG,
    msgid              => :MSG_ID
  );
END;`

	var msgID []byte
	_, err := aq.conn.Exec(
		plsql,
		sql.Named("QUEUE_NAME", aq.Name),
		sql.Named("SUBSCRIBER", subscriberName),
		sql.Named("MSG", go_ora.Out{Dest: message, Size: messageSize}),
		sql.Named("MSG_ID", go_ora.Out{Dest: &msgID, Size: 100}),
	)
	return msgID, err
}

func (aq *AQ) Dequeue(message driver.Value, messageSize int) (messageID []byte, err error) {
	err = aq.validate()
	if err != nil {
		return
	}
	sqlText := `DECLARE
	dequeue_options dbms_aq.dequeue_options_t;
	message_properties	dbms_aq.message_properties_t;
BEGIN
	dequeue_options.VISIBILITY := DBMS_AQ.IMMEDIATE;
	DBMS_AQ.DEQUEUE (
		queue_name => :QUEUE_NAME,
		dequeue_options => dequeue_options,
		message_properties => message_properties,
		payload => :MSG,
		msgid => :MSG_ID);
END;`
	_, err = aq.conn.Exec(
		sqlText,
		sql.Named("QUEUE_NAME", aq.Name),
		sql.Named("MSG", go_ora.Out{Dest: message, Size: messageSize}),
		sql.Named("MSG_ID", go_ora.Out{Dest: &messageID, Size: 100}),
	)
	return
}

func (aq *AQ) Enqueue(message driver.Value) (messageID []byte, err error) {
	err = aq.validate()
	if err != nil {
		return
	}
	sqlText := `DECLARE
	enqueue_options dbms_aq.enqueue_options_t;
	message_properties dbms_aq.message_properties_t;
BEGIN
	DBMS_AQ.ENQUEUE (
	  queue_name => :QUEUE_NAME,
	  enqueue_options => enqueue_options,
	  message_properties => message_properties,
	  payload => :MSG,
	  msgid => :MSG_ID);
END;`
	_, err = aq.conn.Exec(sqlText, sql.Named("QUEUE_NAME", aq.Name),
		sql.Named("MSG", message),
		sql.Named("MSG_ID", go_ora.Out{Dest: &messageID, Size: 100}))
	return
}
