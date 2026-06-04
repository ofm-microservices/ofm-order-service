package application

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-service/internal/domain"
)

type testBroker struct {
	subjects []string
	payloads [][]byte
	err      error
}

func (b *testBroker) Publish(_ context.Context, subject string, payload []byte) error {
	b.subjects = append(b.subjects, subject)
	b.payloads = append(b.payloads, append([]byte(nil), payload...))
	return b.err
}

func (b *testBroker) Subscribe(context.Context, string, string, MessageHandler) error { return nil }
func (b *testBroker) Close()                                                          {}

type testOrders struct {
	createParams   []domain.CreateOrderParams
	createErr      error
	fundedCalls    []string
	failedCalls    []string
	saveCheckout   []string
	snap           *domain.Order
	upserted       []*domain.Order
	saveQuestions  []domain.SaveQuestionSnapshotsParams
	saveReqAnswers []domain.SaveRequirementAnswersParams
	requirements   *domain.OrderRequirements
}

func (r *testOrders) Create(_ context.Context, params domain.CreateOrderParams) (*domain.Order, error) {
	r.createParams = append(r.createParams, params)
	if r.createErr != nil {
		return nil, r.createErr
	}
	return &domain.Order{
		OrderID:             params.OrderID,
		SagaID:              params.SagaID,
		BuyerID:             params.BuyerID,
		SellerID:            params.SellerID,
		GigID:               params.GigID,
		GigTitle:            params.GigTitle,
		PackageID:           params.PackageID,
		PackageTier:         params.PackageTier,
		PackageDescription:  params.PackageDescription,
		PackageDeliveryDays: params.PackageDeliveryDays,
		PriceCents:          params.PriceCents,
		Currency:            params.Currency,
		Status:              domain.OrderStatusRequirementsPending,
	}, nil
}

func (r *testOrders) SaveQuestionSnapshots(_ context.Context, params domain.SaveQuestionSnapshotsParams) error {
	r.saveQuestions = append(r.saveQuestions, params)
	return nil
}

func (r *testOrders) GetByID(_ context.Context, orderID string) (*domain.Order, error) {
	if r.snap != nil {
		return r.snap, nil
	}
	return &domain.Order{OrderID: orderID, Status: domain.OrderStatusRequirementsPending}, nil
}

func (r *testOrders) HasConfirmPrerequisites(context.Context, string) (bool, bool, error) {
	return true, true, nil
}

func (r *testOrders) MarkPaid(context.Context, string, string) (*domain.Order, error) {
	return nil, nil
}

func (r *testOrders) MarkFunded(_ context.Context, orderID, paymentIntentID string) (*domain.Order, error) {
	r.fundedCalls = append(r.fundedCalls, orderID+":"+paymentIntentID)
	return &domain.Order{OrderID: orderID, Status: domain.OrderStatusFunded}, nil
}

func (r *testOrders) MarkFailed(_ context.Context, orderID, reason string) (*domain.Order, error) {
	r.failedCalls = append(r.failedCalls, orderID+":"+reason)
	return &domain.Order{OrderID: orderID, Status: domain.OrderStatusFailed, FailureReason: reason}, nil
}

func (r *testOrders) SaveRequirementAnswers(_ context.Context, params domain.SaveRequirementAnswersParams) (*domain.Order, error) {
	r.saveReqAnswers = append(r.saveReqAnswers, params)
	return &domain.Order{OrderID: params.OrderID, Status: domain.OrderStatusRequirementsCompleted}, nil
}

func (r *testOrders) SaveBuyerInitialMessage(context.Context, domain.SaveBuyerInitialMessageParams) (*domain.Order, error) {
	return nil, nil
}

func (r *testOrders) GetRequirementsByID(_ context.Context, orderID string) (*domain.OrderRequirements, error) {
	if r.requirements != nil {
		return r.requirements, nil
	}
	return &domain.OrderRequirements{
		OrderID:  orderID,
		BuyerID:  "buyer-1",
		SellerID: "seller-1",
		QuestionsAnswers: []domain.OrderRequirementQuestionAnswer{
			{
				Question: &domain.OrderRequirementQuestion{QuestionID: "question-1", Text: "Question?", Type: "text", Required: true, SortOrder: 1},
				Answer:   &domain.OrderRequirementAnswer{Value: "Answer"},
			},
		},
		CustomerMessage: &domain.OrderRequirementCustomerMessage{Message: "Hello", CreatedAt: time.Unix(10, 0).UTC(), UpdatedAt: time.Unix(20, 0).UTC()},
	}, nil
}

func (r *testOrders) AttachFile(context.Context, domain.AttachFileParams) (*domain.Order, error) {
	return nil, nil
}

func (r *testOrders) SaveCheckoutSession(_ context.Context, orderID, paymentIntentID, checkoutURL string) error {
	r.saveCheckout = append(r.saveCheckout, orderID+":"+paymentIntentID+":"+checkoutURL)
	return nil
}

func (r *testOrders) GetLifecycleSnapshot(_ context.Context, orderID string) (*domain.Order, error) {
	if r.snap != nil {
		return r.snap, nil
	}
	return &domain.Order{OrderID: orderID, Status: domain.OrderStatusDelivered}, nil
}

func (r *testOrders) SaveDelivery(context.Context, domain.SaveDeliveryParams) (*domain.Order, error) {
	return &domain.Order{Status: domain.OrderStatusDelivered}, nil
}

func (r *testOrders) MarkReleasePending(_ context.Context, orderID, paymentReleaseID string) (*domain.Order, error) {
	return &domain.Order{OrderID: orderID, PaymentReleaseID: paymentReleaseID, Status: domain.OrderStatusReleasePending}, nil
}

func (r *testOrders) RequestRevision(context.Context, domain.RequestRevisionParams) (*domain.Order, error) {
	return nil, nil
}

func (r *testOrders) OpenDispute(context.Context, domain.OpenDisputeParams) (*domain.Order, error) {
	return nil, nil
}

func (r *testOrders) MarkCompleted(_ context.Context, orderID, paymentReleaseID string) (*domain.Order, error) {
	return &domain.Order{OrderID: orderID, PaymentReleaseID: paymentReleaseID, Status: domain.OrderStatusCompleted}, nil
}

func (r *testOrders) MarkReleaseFailed(_ context.Context, orderID, reason string) (*domain.Order, error) {
	return &domain.Order{OrderID: orderID, FailureReason: reason, Status: domain.OrderStatusReleaseFailed}, nil
}

type testRead struct {
	orders       []*domain.Order
	requirements *domain.OrderRequirements
}

func (r *testRead) Upsert(_ context.Context, order *domain.Order) error {
	r.orders = append(r.orders, order)
	return nil
}

func (r *testRead) GetByID(_ context.Context, orderID string) (*domain.Order, error) {
	if len(r.orders) > 0 {
		return r.orders[len(r.orders)-1], nil
	}
	return &domain.Order{OrderID: orderID, Status: domain.OrderStatusRequirementsPending}, nil
}

func (r *testRead) GetPreviewByID(_ context.Context, orderID string) (*domain.OrderPreview, error) {
	if len(r.orders) > 0 {
		last := r.orders[len(r.orders)-1]
		return &domain.OrderPreview{OrderID: last.OrderID, CreatedAt: last.CreatedAt, Status: last.Status}, nil
	}
	return &domain.OrderPreview{OrderID: orderID, CreatedAt: time.Now().UTC(), Status: domain.OrderStatusRequirementsPending}, nil
}

func (r *testRead) UpsertRequirements(context.Context, string, *domain.OrderRequirements) error { return nil }

func (r *testRead) GetRequirementsByID(_ context.Context, orderID string) (*domain.OrderRequirements, error) {
	if r.requirements != nil {
		return r.requirements, nil
	}
	return &domain.OrderRequirements{OrderID: orderID, BuyerID: "buyer-1", SellerID: "seller-1"}, nil
}

type testLogger struct{}

func (testLogger) Debug(string, ...logging.Field)       {}
func (testLogger) Info(string, ...logging.Field)        {}
func (testLogger) Warn(string, ...logging.Field)        {}
func (testLogger) Error(string, ...logging.Field)       {}
func (testLogger) With(...logging.Field) logging.Logger { return testLogger{} }
func (testLogger) Sync() error                          { return nil }

type testFiles struct {
	url string
	err error
}

func (f *testFiles) GetFileURL(context.Context, string) (string, error) { return f.url, f.err }
func (f *testFiles) Close() error                                       { return nil }

type testUsers struct {
	preview map[string]*UserPreview
	err     error
}

func (u *testUsers) GetUserPreviewByIDNoCache(_ context.Context, userID string) (*UserPreview, error) {
	if u.err != nil {
		return nil, u.err
	}
	if u.preview == nil {
		return nil, nil
	}
	return u.preview[userID], nil
}

func (u *testUsers) Close() error { return nil }

func TestNewRejectsNilLogger(t *testing.T) {
	orders := &testOrders{}
	read := &testRead{}
	files := &testFiles{}
	users := &testUsers{}
	broker := &testBroker{}

	svc, err := New(orders, read, files, users, broker, Config{}, nil)
	if !errors.Is(err, ErrNilLogger) {
		t.Fatalf("err = %v, want ErrNilLogger", err)
	}
	if svc != nil {
		t.Fatalf("svc = %#v, want nil", svc)
	}
}

func TestCreatePublishesSuccessAndUpsertsReadModel(t *testing.T) {
	orders := &testOrders{}
	read := &testRead{}
	files := &testFiles{}
	users := &testUsers{preview: map[string]*UserPreview{
		"buyer-1":  {UserID: "buyer-1", Username: "buyer", DisplayName: "Buyer", AvatarURL: "buyer.png"},
		"seller-1": {UserID: "seller-1", Username: "seller", DisplayName: "Seller", AvatarURL: "seller.png"},
	}}
	broker := &testBroker{}

	svc, err := New(orders, read, files, users, broker, Config{OrderCreateResultSubject: "order.create.result"}, testLogger{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = svc.Create(context.Background(), CreateOrderCommand{
		SagaID:              "saga-1",
		OrderID:             "order-1",
		BuyerID:             "buyer-1",
		SellerID:            "seller-1",
		GigID:               "gig-1",
		GigTitle:            "Gig",
		PackageID:           "pkg-1",
		PackageTier:         "Basic",
		PackageDescription:  "desc",
		PackageDeliveryDays: 3,
		PriceCents:          1000,
		Currency:            "usd",
		IdempotencyKey:      "idem-1",
		RequestedAt:         "2026-05-24T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := len(orders.createParams); got != 1 {
		t.Fatalf("create calls = %d, want 1", got)
	}
	if got := len(read.orders); got != 1 {
		t.Fatalf("upsert calls = %d, want 1", got)
	}
	if got := broker.subjects; len(got) != 2 || got[0] != "order.projection.preview" || got[1] != "order.create.result" {
		t.Fatalf("subjects = %#v, want [order.projection.preview order.create.result]", got)
	}
	var preview domain.Order
	if err := json.Unmarshal(broker.payloads[0], &preview); err != nil {
		t.Fatalf("unmarshal preview: %v", err)
	}
	if preview.Status != domain.OrderStatusRequirementsPending {
		t.Fatalf("preview status = %v, want %v", preview.Status, domain.OrderStatusRequirementsPending)
	}
	var result map[string]any
	if err := json.Unmarshal(broker.payloads[1], &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if status, _ := result["status"].(string); status != "success" {
		t.Fatalf("status = %v, want success", result["status"])
	}
}

func TestCreatePublishesFailureResultWhenCreateFails(t *testing.T) {
	orders := &testOrders{createErr: errors.New("boom")}
	read := &testRead{}
	files := &testFiles{}
	users := &testUsers{}
	broker := &testBroker{}

	svc, err := New(orders, read, files, users, broker, Config{OrderCreateResultSubject: "order.create.result"}, testLogger{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = svc.Create(context.Background(), CreateOrderCommand{OrderID: "order-1", SagaID: "saga-1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := len(read.orders); got != 0 {
		t.Fatalf("upsert calls = %d, want 0", got)
	}
	if got := broker.subjects; len(got) != 1 || got[0] != "order.create.result" {
		t.Fatalf("subjects = %#v, want [order.create.result]", got)
	}
}

func TestStateTransitionsUpdateReadModel(t *testing.T) {
	orders := &testOrders{snap: &domain.Order{OrderID: "order-1", Status: domain.OrderStatusDelivered}}
	read := &testRead{}
	files := &testFiles{}
	users := &testUsers{}
	broker := &testBroker{}

	svc, err := New(orders, read, files, users, broker, Config{}, testLogger{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	funded, err := svc.MarkOrderFunded(context.Background(), MarkOrderFundedCommand{OrderID: "order-1", PaymentIntentID: "pi-1"})
	if err != nil {
		t.Fatalf("MarkOrderFunded: %v", err)
	}
	if funded.Status != domain.OrderStatusFunded {
		t.Fatalf("funded status = %q, want %q", funded.Status, domain.OrderStatusFunded)
	}

	failed, err := svc.MarkPaymentFailed(context.Background(), MarkPaymentFailedCommand{OrderID: "order-1", Reason: "card_declined"})
	if err != nil {
		t.Fatalf("MarkPaymentFailed: %v", err)
	}
	if failed.Status != domain.OrderStatusFailed {
		t.Fatalf("failed status = %q, want %q", failed.Status, domain.OrderStatusFailed)
	}

	delivered, err := svc.SaveDelivery(context.Background(), SaveDeliveryCommand{
		OrderID:     "order-1",
		SellerID:    "seller-1",
		Message:     "done",
		RequestedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("SaveDelivery: %v", err)
	}
	if delivered.Status != domain.OrderStatusDelivered {
		t.Fatalf("delivered status = %q, want %q", delivered.Status, domain.OrderStatusDelivered)
	}

	rel, err := svc.MarkReleasePending(context.Background(), MarkReleasePendingCommand{OrderID: "order-1", PaymentReleaseID: "rel-1"})
	if err != nil {
		t.Fatalf("MarkReleasePending: %v", err)
	}
	if rel.Status != domain.OrderStatusReleasePending {
		t.Fatalf("release pending status = %q, want %q", rel.Status, domain.OrderStatusReleasePending)
	}

	comp, err := svc.MarkOrderCompleted(context.Background(), MarkOrderCompletedCommand{OrderID: "order-1", PaymentReleaseID: "rel-1"})
	if err != nil {
		t.Fatalf("MarkOrderCompleted: %v", err)
	}
	if comp.Status != domain.OrderStatusCompleted {
		t.Fatalf("completed status = %q, want %q", comp.Status, domain.OrderStatusCompleted)
	}

	releaseFailed, err := svc.MarkReleaseFailed(context.Background(), MarkReleaseFailedCommand{OrderID: "order-1", Reason: "boom"})
	if err != nil {
		t.Fatalf("MarkReleaseFailed: %v", err)
	}
	if releaseFailed.Status != domain.OrderStatusReleaseFailed {
		t.Fatalf("release failed status = %q, want %q", releaseFailed.Status, domain.OrderStatusReleaseFailed)
	}
}

func TestGetOrderPreviewByIDHydratesParticipants(t *testing.T) {
	orders := &testOrders{snap: &domain.Order{
		OrderID:       "order-1",
		BuyerID:       "buyer-1",
		SellerID:      "seller-1",
		GigID:         "gig-1",
		GigTitle:      "Gig",
		PackageID:     "pkg-1",
		PackageTier:   "basic",
		PriceCents:    1000,
		Currency:      "usd",
		Status:        domain.OrderStatusFunded,
		PictureFileID: "file-1",
		CreatedAt:     time.Unix(10, 0).UTC(),
		UpdatedAt:     time.Unix(20, 0).UTC(),
	}}
	read := &testRead{orders: []*domain.Order{{
		OrderID:       "order-1",
		BuyerID:       "buyer-1",
		SellerID:      "seller-1",
		GigID:         "gig-1",
		GigTitle:      "Gig",
		PackageID:     "pkg-1",
		PackageTier:   "basic",
		PriceCents:    1000,
		Currency:      "usd",
		Status:        domain.OrderStatusFunded,
		PictureFileID: "file-1",
		CreatedAt:     time.Unix(10, 0).UTC(),
		UpdatedAt:     time.Unix(20, 0).UTC(),
	}}}
	files := &testFiles{url: "https://cdn.example.com/file-1.png"}
	users := &testUsers{preview: map[string]*UserPreview{
		"buyer-1":  {UserID: "buyer-1", Username: "alex1", DisplayName: "Buyer", AvatarURL: "buyer.png"},
		"seller-1": {UserID: "seller-1", Username: "alex2", DisplayName: "Seller", AvatarURL: "seller.png"},
	}}
	broker := &testBroker{}

	svc, err := New(orders, read, files, users, broker, Config{}, testLogger{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	res, err := svc.GetOrderPreviewByID(context.Background(), GetOrderPreviewByIDCommand{OrderID: "order-1", UserID: "seller-1", Role: "seller"})
	if err != nil {
		t.Fatalf("GetOrderPreviewByID: %v", err)
	}
	if res.Customer == nil || res.Freelancer == nil {
		t.Fatalf("participants = %#v, %#v", res.Customer, res.Freelancer)
	}
	if res.Gig == nil || res.Gig.PictureURL != "https://cdn.example.com/file-1.png" {
		t.Fatalf("gig = %#v", res.Gig)
	}
	if got := len(read.orders); got != 2 {
		t.Fatalf("orders tracked = %d, want 2", got)
	}
}

func TestGetOrderRequirementsByIDReturnsSnapshot(t *testing.T) {
	orders := &testOrders{snap: &domain.Order{
		OrderID:  "order-1",
		BuyerID:  "buyer-1",
		SellerID: "seller-1",
	}}
	read := &testRead{orders: []*domain.Order{{OrderID: "order-1", BuyerID: "buyer-1", SellerID: "seller-1"}}}
	read.requirements = &domain.OrderRequirements{
		OrderID: "order-1",
		BuyerID: "buyer-1",
		SellerID: "seller-1",
		QuestionsAnswers: []domain.OrderRequirementQuestionAnswer{{
			Question: &domain.OrderRequirementQuestion{QuestionID: "question-1", Text: "Question?", Type: "text", Required: true, SortOrder: 1},
			Answer:   &domain.OrderRequirementAnswer{Value: "Answer"},
		}},
		CustomerMessage: &domain.OrderRequirementCustomerMessage{Message: "Hello", CreatedAt: time.Unix(10, 0).UTC(), UpdatedAt: time.Unix(20, 0).UTC()},
	}
	svc, err := New(orders, read, &testFiles{}, &testUsers{}, &testBroker{}, Config{}, testLogger{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	res, err := svc.GetOrderRequirementsByID(context.Background(), GetOrderRequirementsByIDCommand{OrderID: "order-1", UserID: "buyer-1"})
	if err != nil {
		t.Fatalf("GetOrderRequirementsByID: %v", err)
	}
	if len(res.QuestionsAnswers) != 1 {
		t.Fatalf("questions answers = %d, want 1", len(res.QuestionsAnswers))
	}
	if res.CustomerMessage == nil || res.CustomerMessage.Message != "Hello" {
		t.Fatalf("customer message = %#v", res.CustomerMessage)
	}
}
