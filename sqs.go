package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Evento que será enviado para a fila
type EvaluationEvent struct {
	UserID    string    `json:"user_id"`
	FlagName  string    `json:"flag_name"`
	Result    bool      `json:"result"`
	Timestamp time.Time `json:"timestamp"`
}

// sendEvaluationEvent envia um evento para a fila SQS.
//
// O aws-sdk-go v1 não tem instrumentação OpenTelemetry oficial, então o span
// PRODUCER é criado aqui e o contexto do trace (traceparent) vai nos
// MessageAttributes da mensagem — o analytics-service extrai de lá e continua o
// mesmo trace, o que liga evaluation -> analytics no Service Map do APM.
func (a *App) sendEvaluationEvent(ctx context.Context, userID, flagName string, result bool) {
	// Se a URL da fila não foi configurada, apenas loga localmente e sai.
	if a.SqsSvc == nil || a.SqsQueueURL == "" {
		slog.InfoContext(ctx, "[SQS_DISABLED] Evento", "user_id", userID, "flag", flagName, "result", result)
		return
	}

	queueName := path.Base(a.SqsQueueURL)
	ctx, span := tracer.Start(ctx, "send "+queueName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "aws_sqs"),
			attribute.String("messaging.operation.type", "send"),
			attribute.String("messaging.destination.name", queueName),
			attribute.String("flag.name", flagName),
		),
	)
	defer span.End()

	event := EvaluationEvent{
		UserID:    userID,
		FlagName:  flagName,
		Result:    result,
		Timestamp: time.Now().UTC(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		slog.ErrorContext(ctx, "Erro ao serializar evento SQS", "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "serialização")
		return
	}

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	attrs := make(map[string]*sqs.MessageAttributeValue, len(carrier))
	for k, v := range carrier {
		attrs[k] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}

	// Envia a mensagem
	out, err := a.SqsSvc.SendMessageWithContext(ctx, &sqs.SendMessageInput{
		MessageBody:       aws.String(string(body)),
		QueueUrl:          aws.String(a.SqsQueueURL),
		MessageAttributes: attrs,
	})

	if err != nil {
		slog.ErrorContext(ctx, "Erro ao enviar mensagem para SQS", "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "falha no SendMessage")
	} else {
		span.SetAttributes(attribute.String("messaging.message.id", aws.StringValue(out.MessageId)))
		slog.InfoContext(ctx, "Evento de avaliação enviado para SQS", "flag", flagName)
	}
}
