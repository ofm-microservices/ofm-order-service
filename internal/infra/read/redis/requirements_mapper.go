package redis

import (
	"strings"
	"time"

	"order-service/internal/domain"
	"order-service/internal/infra/read/redis/model"
)

func mapRequirementsToCache(requirements *domain.OrderRequirements) model.RequirementsCache {
	if requirements == nil {
		return model.RequirementsCache{}
	}
	out := model.RequirementsCache{
		QuestionsAnswers: make([]model.RequirementQuestionAnswer, 0, len(requirements.QuestionsAnswers)),
	}
	for _, qa := range requirements.QuestionsAnswers {
		out.QuestionsAnswers = append(out.QuestionsAnswers, model.RequirementQuestionAnswer{
			Question: mapRequirementQuestionToCache(qa.Question),
			Answer:   mapRequirementAnswerToCache(qa.Answer),
		})
	}
	if requirements.CustomerMessage != nil {
		out.CustomerMessage = &model.RequirementCustomerMessage{
			Message:   strings.TrimSpace(requirements.CustomerMessage.Message),
			CreatedAt: requirements.CustomerMessage.CreatedAt.UTC().Format(time.RFC3339Nano),
			UpdatedAt: requirements.CustomerMessage.UpdatedAt.UTC().Format(time.RFC3339Nano),
		}
	}
	return out
}

func mapRequirementsCacheToDomain(orderID string, cache model.RequirementsCache) *domain.OrderRequirements {
	out := &domain.OrderRequirements{
		OrderID:          strings.TrimSpace(orderID),
		QuestionsAnswers: make([]domain.OrderRequirementQuestionAnswer, 0, len(cache.QuestionsAnswers)),
	}
	for _, qa := range cache.QuestionsAnswers {
		out.QuestionsAnswers = append(out.QuestionsAnswers, domain.OrderRequirementQuestionAnswer{
			Question: mapRequirementQuestionToDomain(qa.Question),
			Answer:   mapRequirementAnswerToDomain(qa.Answer),
		})
	}
	if cache.CustomerMessage != nil {
		out.CustomerMessage = &domain.OrderRequirementCustomerMessage{
			Message:   strings.TrimSpace(cache.CustomerMessage.Message),
			CreatedAt: parseTimeOrZero(cache.CustomerMessage.CreatedAt),
			UpdatedAt: parseTimeOrZero(cache.CustomerMessage.UpdatedAt),
		}
	}
	return out
}

func mapRequirementQuestionToCache(q *domain.OrderRequirementQuestion) *model.RequirementQuestion {
	if q == nil {
		return nil
	}
	return &model.RequirementQuestion{
		QuestionID: strings.TrimSpace(q.QuestionID),
		Text:       strings.TrimSpace(q.Text),
		Type:       strings.TrimSpace(q.Type),
		Required:   q.Required,
		SortOrder:  q.SortOrder,
	}
}

func mapRequirementAnswerToCache(a *domain.OrderRequirementAnswer) *model.RequirementAnswer {
	if a == nil {
		return nil
	}
	return &model.RequirementAnswer{Value: strings.TrimSpace(a.Value)}
}

func mapRequirementQuestionToDomain(q *model.RequirementQuestion) *domain.OrderRequirementQuestion {
	if q == nil {
		return nil
	}
	return &domain.OrderRequirementQuestion{
		QuestionID: strings.TrimSpace(q.QuestionID),
		Text:       strings.TrimSpace(q.Text),
		Type:       strings.TrimSpace(q.Type),
		Required:   q.Required,
		SortOrder:  q.SortOrder,
	}
}

func mapRequirementAnswerToDomain(a *model.RequirementAnswer) *domain.OrderRequirementAnswer {
	if a == nil {
		return nil
	}
	return &domain.OrderRequirementAnswer{Value: strings.TrimSpace(a.Value)}
}
