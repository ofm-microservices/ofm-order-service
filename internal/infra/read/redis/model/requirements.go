package model

// RequirementsCache is the Redis projection model for an order requirements page.
type RequirementsCache struct {
	QuestionsAnswers []RequirementQuestionAnswer `json:"questions_answers"`
	CustomerMessage  *RequirementCustomerMessage `json:"customer_message"`
}

// RequirementQuestionAnswer stores one question and its optional answer in the cache.
type RequirementQuestionAnswer struct {
	Question *RequirementQuestion `json:"question"`
	Answer   *RequirementAnswer   `json:"answer"`
}

// RequirementQuestion stores the immutable question snapshot in the cache.
type RequirementQuestion struct {
	QuestionID string `json:"question_id"`
	Text       string `json:"text"`
	Type       string `json:"type"`
	Required   bool   `json:"required"`
	SortOrder  int32  `json:"sort_order"`
}

// RequirementAnswer stores one buyer answer snapshot in the cache.
type RequirementAnswer struct {
	Value string `json:"value"`
}

// RequirementCustomerMessage stores the buyer message snapshot in the cache.
type RequirementCustomerMessage struct {
	Message   string `json:"message"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}
