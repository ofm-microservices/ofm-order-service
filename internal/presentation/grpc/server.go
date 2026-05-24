package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	orderwritev1 "github.com/ofm-microservices/ofm-common/proto/orderwrite/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcpkg "google.golang.org/grpc"
	"order-service/config"
	app "order-service/internal/application"
)

type server struct {
	orderwritev1.UnimplementedOrderWriteServiceServer
	svc      app.Service
	cfg      config.GRPCConfig
	log      logging.Logger
	srv      *grpcpkg.Server
	listener net.Listener
}

func NewServer(svc app.Service, cfg config.GRPCConfig, log logging.Logger) (Server, error) {
	if svc == nil {
		return nil, ErrNilService
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	grpcSrv := grpcpkg.NewServer(grpcpkg.StatsHandler(otelgrpc.NewServerHandler()), grpcpkg.UnaryInterceptor(metrics.UnaryServerInterceptor()))
	s := &server{svc: svc, cfg: cfg, log: log.With(logging.String("module", "grpc-order-server")), srv: grpcSrv}
	orderwritev1.RegisterOrderWriteServiceServer(grpcSrv, s)
	return s, nil
}
func (s *server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = lis
	s.log.Info("starting grpc server", logging.String("addr", addr))
	return s.srv.Serve(lis)
}
func (s *server) Shutdown(context.Context) error {
	if s.srv != nil {
		s.srv.GracefulStop()
	}
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *server) CreateDraftOrder(ctx context.Context, req *orderwritev1.CreateDraftOrderRequest) (*orderwritev1.CreateDraftOrderResponse, error) {
	questions := make([]app.OrderQuestionSnapshot, 0, len(req.GetQuestions()))
	for _, q := range req.GetQuestions() {
		optionsJSON, err := json.Marshal(q.GetOptions())
		if err != nil {
			return nil, err
		}
		questions = append(questions, app.OrderQuestionSnapshot{
			QuestionID:  q.GetQuestionId(),
			Text:        q.GetText(),
			Type:        q.GetType(),
			Required:    q.GetRequired(),
			OptionsJSON: string(optionsJSON),
			SortOrder:   q.GetSortOrder(),
		})
	}
	res, err := s.svc.CreateDraftOrder(ctx, app.CreateDraftOrderCommand{SagaID: req.GetSagaId(), OrderID: req.GetOrderId(), BuyerID: req.GetBuyerUserId(), SellerID: req.GetSellerUserId(), GigID: req.GetGigId(), GigTitle: req.GetGigTitleSnapshot(), PackageID: req.GetPackageId(), PackageTier: req.GetPackageTitleSnapshot(), PackageDescription: req.GetPackageDescriptionSnapshot(), PriceCents: req.GetPriceAmountSnapshot(), Currency: req.GetPriceCurrencySnapshot(), PackageDeliveryDays: req.GetDeliveryDaysSnapshot(), Questions: questions, IdempotencyKey: req.GetIdempotencyKey(), RequestedAt: req.GetRequestedAt()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.CreateDraftOrderResponse{Order: &orderwritev1.OrderSnapshot{OrderId: res.OrderID, Status: res.Status}, Questions: req.GetQuestions()}, nil
}
func (s *server) SaveRequirementAnswers(ctx context.Context, req *orderwritev1.SaveRequirementAnswersRequest) (*orderwritev1.SaveRequirementAnswersResponse, error) {
	answers := make([]app.OrderAnswer, 0, len(req.GetAnswers()))
	for _, a := range req.GetAnswers() {
		answers = append(answers, app.OrderAnswer{QuestionID: a.GetQuestionId(), Value: a.GetValue()})
	}
	res, err := s.svc.SaveRequirementAnswers(ctx, app.SaveRequirementAnswersCommand{OrderID: req.GetOrderId(), Answers: answers})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.SaveRequirementAnswersResponse{Order: &orderwritev1.OrderSnapshot{OrderId: res.OrderID, Status: res.Status}}, nil
}
func (s *server) SaveBuyerInitialMessage(ctx context.Context, req *orderwritev1.SaveBuyerInitialMessageRequest) (*orderwritev1.SaveBuyerInitialMessageResponse, error) {
	res, err := s.svc.SaveBuyerInitialMessage(ctx, app.SaveBuyerInitialMessageCommand{OrderID: req.GetOrderId(), Message: req.GetMessage()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.SaveBuyerInitialMessageResponse{Order: &orderwritev1.OrderSnapshot{OrderId: res.OrderID, Status: res.Status}}, nil
}
func (s *server) AttachFileToOrder(ctx context.Context, req *orderwritev1.AttachFileToOrderRequest) (*orderwritev1.AttachFileToOrderResponse, error) {
	res, err := s.svc.AttachFile(ctx, app.AttachFileCommand{OrderID: req.GetOrderId(), AttachmentID: req.GetAttachmentId()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.AttachFileToOrderResponse{Order: &orderwritev1.OrderSnapshot{OrderId: res.OrderID, Status: res.Status}}, nil
}
func (s *server) GetOrderPaymentSnapshot(ctx context.Context, req *orderwritev1.GetOrderPaymentSnapshotRequest) (*orderwritev1.GetOrderPaymentSnapshotResponse, error) {
	snap, err := s.svc.GetOrderPaymentSnapshot(ctx, req.GetOrderId())
	if err != nil {
		return nil, err
	}
	return &orderwritev1.GetOrderPaymentSnapshotResponse{Order: &orderwritev1.OrderSnapshot{OrderId: snap.OrderID, SagaId: snap.SagaID, BuyerUserId: snap.BuyerID, SellerUserId: snap.SellerID, GigTitleSnapshot: snap.GigTitle, PackageTitleSnapshot: snap.PackageTitle, PriceAmountSnapshot: snap.PriceCents, PriceCurrencySnapshot: snap.Currency, Status: snap.Status}}, nil
}
func (s *server) MarkPaymentPending(ctx context.Context, req *orderwritev1.MarkPaymentPendingRequest) (*orderwritev1.MarkPaymentPendingResponse, error) {
	_, err := s.svc.MarkPaymentPending(ctx, app.MarkPaymentPendingCommand{OrderID: req.GetOrderId(), PaymentIntentID: req.GetPaymentId(), CheckoutURL: req.GetCheckoutUrl(), OccurredAt: req.GetRequestedAt()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.MarkPaymentPendingResponse{Order: &orderwritev1.OrderSnapshot{OrderId: req.GetOrderId(), Status: "payment_pending"}}, nil
}
func (s *server) MarkOrderFunded(ctx context.Context, req *orderwritev1.MarkOrderFundedRequest) (*orderwritev1.MarkOrderFundedResponse, error) {
	_, err := s.svc.MarkOrderFunded(ctx, app.MarkOrderFundedCommand{OrderID: req.GetOrderId(), PaymentIntentID: req.GetPaymentId(), OccurredAt: req.GetRequestedAt()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.MarkOrderFundedResponse{Order: &orderwritev1.OrderSnapshot{OrderId: req.GetOrderId(), Status: "funded"}}, nil
}
func (s *server) MarkPaymentFailed(ctx context.Context, req *orderwritev1.MarkPaymentFailedRequest) (*orderwritev1.MarkPaymentFailedResponse, error) {
	_, err := s.svc.MarkPaymentFailed(ctx, app.MarkPaymentFailedCommand{OrderID: req.GetOrderId(), Reason: req.GetReason()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.MarkPaymentFailedResponse{Order: &orderwritev1.OrderSnapshot{OrderId: req.GetOrderId(), Status: "failed"}}, nil
}

func (s *server) GetOrderLifecycleSnapshot(ctx context.Context, req *orderwritev1.GetOrderLifecycleSnapshotRequest) (*orderwritev1.GetOrderLifecycleSnapshotResponse, error) {
	snap, err := s.svc.GetOrderLifecycleSnapshot(ctx, req.GetOrderId())
	if err != nil {
		return nil, err
	}
	return &orderwritev1.GetOrderLifecycleSnapshotResponse{Order: &orderwritev1.OrderSnapshot{
		OrderId:                    snap.OrderID,
		SagaId:                     snap.SagaID,
		BuyerUserId:                snap.BuyerID,
		SellerUserId:               snap.SellerID,
		GigId:                      snap.GigID,
		GigTitleSnapshot:           snap.GigTitle,
		PackageId:                  snap.PackageID,
		PackageTitleSnapshot:       snap.PackageTitle,
		PackageDescriptionSnapshot: snap.PackageDescription,
		PriceAmountSnapshot:        snap.PriceCents,
		PriceCurrencySnapshot:      snap.Currency,
		Status:                     snap.Status,
		RevisionCountSnapshot:      snap.RevisionCountSnapshot,
		RevisionCountUsed:          snap.RevisionCountUsed,
		BuyerResponseDeadline:      snap.BuyerResponseDeadline,
		PaymentIntentId:            snap.PaymentIntentID,
		PaymentReleaseId:           snap.PaymentReleaseID,
		DeliveredAt:                snap.DeliveredAt,
		CompletedAt:                snap.CompletedAt,
		DisputedAt:                 snap.DisputedAt,
	}}, nil
}

func (s *server) SaveDelivery(ctx context.Context, req *orderwritev1.SaveDeliveryRequest) (*orderwritev1.SaveDeliveryResponse, error) {
	res, err := s.svc.SaveDelivery(ctx, app.SaveDeliveryCommand{OrderID: req.GetOrderId(), SellerID: req.GetSellerUserId(), Message: req.GetDeliveryMessage(), AttachmentIDs: req.GetAttachmentIds(), RequestedAt: req.GetRequestedAt()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.SaveDeliveryResponse{Order: &orderwritev1.OrderSnapshot{OrderId: req.GetOrderId(), Status: res.Status}}, nil
}

func (s *server) MarkReleasePending(ctx context.Context, req *orderwritev1.MarkReleasePendingRequest) (*orderwritev1.MarkReleasePendingResponse, error) {
	res, err := s.svc.MarkReleasePending(ctx, app.MarkReleasePendingCommand{OrderID: req.GetOrderId(), PaymentReleaseID: req.GetPaymentReleaseId(), RequestedAt: req.GetRequestedAt()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.MarkReleasePendingResponse{Order: &orderwritev1.OrderSnapshot{OrderId: req.GetOrderId(), Status: res.Status}}, nil
}

func (s *server) RequestRevision(ctx context.Context, req *orderwritev1.RequestRevisionRequest) (*orderwritev1.RequestRevisionResponse, error) {
	res, err := s.svc.RequestRevision(ctx, app.RequestRevisionCommand{OrderID: req.GetOrderId(), BuyerID: req.GetBuyerUserId(), Reason: req.GetReason(), RequestedAt: req.GetRequestedAt()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.RequestRevisionResponse{Order: &orderwritev1.OrderSnapshot{OrderId: req.GetOrderId(), Status: res.Status}}, nil
}

func (s *server) OpenDispute(ctx context.Context, req *orderwritev1.OpenDisputeRequest) (*orderwritev1.OpenDisputeResponse, error) {
	res, err := s.svc.OpenDispute(ctx, app.OpenDisputeCommand{OrderID: req.GetOrderId(), BuyerID: req.GetBuyerUserId(), Reason: req.GetReason(), RequestedAt: req.GetRequestedAt()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.OpenDisputeResponse{Order: &orderwritev1.OrderSnapshot{OrderId: req.GetOrderId(), Status: res.Status}}, nil
}

func (s *server) MarkOrderCompleted(ctx context.Context, req *orderwritev1.MarkOrderCompletedRequest) (*orderwritev1.MarkOrderCompletedResponse, error) {
	res, err := s.svc.MarkOrderCompleted(ctx, app.MarkOrderCompletedCommand{OrderID: req.GetOrderId(), PaymentReleaseID: req.GetPaymentReleaseId(), OccurredAt: req.GetRequestedAt()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.MarkOrderCompletedResponse{Order: &orderwritev1.OrderSnapshot{OrderId: req.GetOrderId(), Status: res.Status}}, nil
}

func (s *server) MarkReleaseFailed(ctx context.Context, req *orderwritev1.MarkReleaseFailedRequest) (*orderwritev1.MarkReleaseFailedResponse, error) {
	res, err := s.svc.MarkReleaseFailed(ctx, app.MarkReleaseFailedCommand{OrderID: req.GetOrderId(), Reason: req.GetReason(), OccurredAt: req.GetRequestedAt()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.MarkReleaseFailedResponse{Order: &orderwritev1.OrderSnapshot{OrderId: req.GetOrderId(), Status: res.Status}}, nil
}
