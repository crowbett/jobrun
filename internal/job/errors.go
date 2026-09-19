package job

import "errors"

// Sentinel errors. Adapters match them with errors.Is and map them to status codes:
// InvalidArgument (ErrInvalidArgument, ErrInvalidCursor), NotFound (ErrNotFound,
// ErrScheduleNotFound), FailedPrecondition (ErrNotCancellable, ErrNotRetryable),
// AlreadyExists (ErrIdempotencyConflict, ErrScheduleExists), Internal otherwise.
var (
	// ErrNotFound is returned when no job has the requested id.
	ErrNotFound = errors.New("job not found")
	// ErrIdempotencyConflict is returned when an idempotency key is reused on the
	// same queue with a different job body.
	ErrIdempotencyConflict = errors.New("idempotency key reused")
	// ErrNotRetryable is returned by Retry when the job is not dead or cancelled.
	ErrNotRetryable = errors.New("job is not in a retryable state")
	// ErrNotCancellable is returned by Cancel when the job is already finished.
	ErrNotCancellable = errors.New("job is not in a cancellable state")
	// ErrLeaseLost is returned by Complete/Fail when the caller no longer owns
	// the lease (it expired and was reaped, or the job was cancelled).
	ErrLeaseLost = errors.New("lease lost")
	// ErrNoHandler is returned by the worker when a claimed job has a type
	// nothing is registered for.
	ErrNoHandler = errors.New("no handler registered for type")
	// ErrPermanent marks a handler failure that must not be retried: the job
	// goes straight to dead. Handlers wrap it: fmt.Errorf("%w: bad payload", job.ErrPermanent).
	ErrPermanent = errors.New("permanent failure")

	// ErrInvalidArgument wraps every validation failure (see Validate methods).
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrInvalidCursor is returned by DecodeCursor for a page token that was not
	// produced by Cursor.Encode.
	ErrInvalidCursor = errors.New("invalid cursor")
	// ErrInvalidTransition is returned by Status.Transition for an edge that is
	// not in the transition table.
	ErrInvalidTransition = errors.New("invalid status transition")
	// ErrDuplicateHandler is returned by Registry.Register when the type already
	// has a handler.
	ErrDuplicateHandler = errors.New("handler already registered for type")
	// ErrScheduleNotFound is returned when no schedule has the requested id.
	ErrScheduleNotFound = errors.New("schedule not found")
	// ErrScheduleExists is returned when a schedule name is already taken.
	ErrScheduleExists = errors.New("schedule name already exists")
)
