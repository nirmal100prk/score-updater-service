package errorhandler

type CustomError struct {
	Code             int    `json:"code"`
	Message          string `json:"message"`
	CustomErrMessage string `json:"customerrormessage"`
	Err              error
}

func (r *CustomError) Error() string {
	return r.CustomErrMessage
}

func NewCustomError(code int, message string) *CustomError {
	return &CustomError{
		Code:    code,
		Message: message,
	}
}
