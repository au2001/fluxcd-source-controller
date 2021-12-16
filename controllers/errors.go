package controllers

type stallingError struct {
	// Reason is the stalled condition reason string.
	Reason string
	// Err is the error that caused stalling. This can be used as the message in
	// stalled condition.
	Err error
}

func (se *stallingError) Error() string {
	return se.Err.Error()
}

func (se *stallingError) Unwrap() error {
	return se.Err
}
