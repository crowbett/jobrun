package server

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/crowbett/jobrun/internal/interceptor"
	"github.com/crowbett/jobrun/internal/job"
)

// internalMsg is the only message an unmapped error ever leaks to a client.
const internalMsg = "internal error"

// toStatus maps a domain error to a gRPC status. Known sentinels keep their
// message (it is client-safe by construction); anything else becomes Internal
// with a fixed message and is logged with the request id and the real cause.
func toStatus(ctx context.Context, log *slog.Logger, method string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err // already a status (e.g. from validation or an interceptor)
	}
	switch {
	case errors.Is(err, job.ErrInvalidArgument), errors.Is(err, job.ErrInvalidCursor):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, job.ErrNotFound), errors.Is(err, job.ErrScheduleNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, job.ErrInvalidTransition), errors.Is(err, job.ErrNotCancellable), errors.Is(err, job.ErrNotRetryable):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, job.ErrScheduleExists), errors.Is(err, job.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}
	log.ErrorContext(ctx, "unhandled error",
		slog.String("method", method),
		slog.String("request_id", interceptor.RequestIDFrom(ctx)),
		slog.String("error", err.Error()))
	return status.Error(codes.Internal, internalMsg)
}

// invalid builds an InvalidArgument status from a validation message.
func invalid(msg string) error { return status.Error(codes.InvalidArgument, msg) }
