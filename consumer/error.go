package consumer

type Kind int

const (
	_ Kind = iota
	Retryable
	Fatal
)

type CErr interface {
	error
	Kind() Kind
}

type cerr struct {
	err  error
	kind Kind
}

func (e cerr) Error() string {
	return e.err.Error()
}

func (e cerr) Unwrap() error {
	return e.err
}

func (e cerr) Kind() Kind {
	return e.kind
}

func RetryableErr(err error) error {
	return cerr{err: err, kind: Retryable}
}

func FatalErr(err error) error {
	return cerr{err: err, kind: Fatal}
}
