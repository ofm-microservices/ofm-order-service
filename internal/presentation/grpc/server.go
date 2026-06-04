package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	commonv1 "github.com/ofm-microservices/ofm-common/proto/common/v1"
	orderwritev1 "github.com/ofm-microservices/ofm-common/proto/orderwrite/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcpkg "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"order-service/config"
	app "order-service/internal/application"
	"order-service/internal/domain"
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
	res, err := s.svc.CreateDraftOrder(ctx, app.CreateDraftOrderCommand{SagaID: req.GetSagaId(), OrderID: req.GetOrderId(), BuyerID: req.GetBuyerUserId(), SellerID: req.GetSellerUserId(), SellerUsername: req.GetSellerUsername(), GigID: req.GetGigId(), GigTitle: req.GetGigTitleSnapshot(), PictureFileID: req.GetPictureFileId(), PackageID: req.GetPackageId(), PackageTier: req.GetPackageTitleSnapshot(), PackageDescription: req.GetPackageDescriptionSnapshot(), PriceCents: req.GetPriceAmountSnapshot(), Currency: req.GetPriceCurrencySnapshot(), PackageDeliveryDays: req.GetDeliveryDaysSnapshot(), Questions: questions, IdempotencyKey: req.GetIdempotencyKey(), RequestedAt: req.GetRequestedAt()})
	if err != nil {
		return nil, err
	}
	return &orderwritev1.CreateDraftOrderResponse{Order: &orderwritev1.OrderSnapshot{OrderId: res.OrderID, SellerUsername: req.GetSellerUsername(), Status: res.Status}, Questions: req.GetQuestions()}, nil
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
	return &orderwritev1.GetOrderPaymentSnapshotResponse{
		Order: &orderwritev1.OrderSnapshot{
			OrderId:               snap.OrderID,
			SagaId:                snap.SagaID,
			BuyerUserId:           snap.BuyerID,
			SellerUserId:          snap.SellerID,
			SellerUsername:        snap.SellerUsername,
			GigTitleSnapshot:      snap.GigTitle,
			PackageTitleSnapshot:  snap.PackageTitle,
			PriceAmountSnapshot:   snap.PriceCents,
			PriceCurrencySnapshot: snap.Currency,
			Status:                snap.Status,
		},
		RequirementsCompleted: snap.RequirementsCompleted,
		MessageCompleted:      snap.MessageCompleted,
	}, nil
}

func (s *server) GetOrderPreviewByID(ctx context.Context, req *orderwritev1.GetOrderPreviewByIDRequest) (*orderwritev1.GetOrderPreviewByIDResponse, error) {
	role := ""
	switch req.GetRole() {
	case commonv1.ParticipantRole_PARTICIPANT_ROLE_BUYER:
		role = "buyer"
	case commonv1.ParticipantRole_PARTICIPANT_ROLE_SELLER:
		role = "seller"
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid participant role")
	}
	res, err := s.svc.GetOrderPreviewByID(ctx, app.GetOrderPreviewByIDCommand{
		OrderID: req.GetOrderId(),
		UserID:  req.GetUserId(),
		Role:    role,
	})
	if err != nil {
		if errors.Is(err, domain.ErrOrderNotFound) {
			return nil, status.Error(codes.NotFound, domain.ErrOrderNotFound.Error())
		}
		if errors.Is(err, domain.ErrOrderNotOwned) {
			return nil, status.Error(codes.PermissionDenied, domain.ErrOrderNotOwned.Error())
		}
		return nil, status.Error(codes.Internal, "internal server error")
	}
	resp := &orderwritev1.GetOrderPreviewByIDResponse{}
	if res.Order != nil {
		resp.Order = &orderwritev1.OrderPreview{
			OrderId:   res.Order.OrderID,
			CreatedAt: res.Order.CreatedAt,
			Status:    res.Order.Status,
		}
	}
	if res.Gig != nil {
		resp.Gig = &orderwritev1.OrderGigSnapshot{
			GigId:               res.Gig.GigID,
			Title:               res.Gig.Title,
			PictureFileId:       res.Gig.PictureFileID,
			PictureUrl:          res.Gig.PictureURL,
			PackageId:           res.Gig.PackageID,
			PackageTitle:        res.Gig.PackageTitle,
			PriceCents:          res.Gig.PriceCents,
			Currency:            res.Gig.Currency,
			Description:         res.Gig.Description,
			PackageDeliveryDays: res.Gig.PackageDeliveryDays,
		}
	}
	if res.Customer != nil {
		resp.Customer = &orderwritev1.OrderUserSnapshot{
			UserId:      res.Customer.UserID,
			Username:    res.Customer.Username,
			DisplayName: res.Customer.DisplayName,
			AvatarUrl:   res.Customer.AvatarURL,
		}
	}
	if res.Freelancer != nil {
		resp.Freelancer = &orderwritev1.OrderUserSnapshot{
			UserId:      res.Freelancer.UserID,
			Username:    res.Freelancer.Username,
			DisplayName: res.Freelancer.DisplayName,
			AvatarUrl:   res.Freelancer.AvatarURL,
		}
	}
	return resp, nil
}

func (s *server) GetOrderRequirementsByID(ctx context.Context, req *orderwritev1.GetOrderRequirementsByIDRequest) (*orderwritev1.GetOrderRequirementsByIDResponse, error) {
	if strings.TrimSpace(req.GetOrderId()) == "" || strings.TrimSpace(req.GetUserId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid order requirements request")
	}
	res, err := s.svc.GetOrderRequirementsByID(ctx, app.GetOrderRequirementsByIDCommand{
		OrderID: req.GetOrderId(),
		UserID:  req.GetUserId(),
	})
	if err != nil {
		if errors.Is(err, domain.ErrOrderNotFound) {
			return nil, status.Error(codes.NotFound, domain.ErrOrderNotFound.Error())
		}
		return nil, status.Error(codes.Internal, "internal server error")
	}
	resp := &orderwritev1.GetOrderRequirementsByIDResponse{
		QuestionsAnswers: make([]*orderwritev1.OrderRequirementQuestionAnswer, 0, len(res.QuestionsAnswers)),
	}
	for _, qa := range res.QuestionsAnswers {
		resp.QuestionsAnswers = append(resp.QuestionsAnswers, &orderwritev1.OrderRequirementQuestionAnswer{
			Question: toRequirementQuestionProto(qa.Question),
			Answer:   toRequirementAnswerProto(qa.Answer),
		})
	}
	if res.CustomerMessage != nil {
		resp.CustomerMessage = &orderwritev1.OrderRequirementCustomerMessage{
			Message:   res.CustomerMessage.Message,
			CreatedAt: res.CustomerMessage.CreatedAt,
			UpdatedAt: res.CustomerMessage.UpdatedAt,
		}
	}
	return resp, nil
}

func (s *server) GetOrderDeliveryByID(ctx context.Context, req *orderwritev1.GetOrderDeliveryByIDRequest) (*orderwritev1.GetOrderDeliveryByIDResponse, error) {
	if strings.TrimSpace(req.GetOrderId()) == "" || strings.TrimSpace(req.GetUserId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid order delivery request")
	}
	res, err := s.svc.GetOrderDeliveryByID(ctx, app.GetOrderDeliveryByIDCommand{
		OrderID: req.GetOrderId(),
		UserID:  req.GetUserId(),
	})
	if err != nil {
		if errors.Is(err, domain.ErrOrderNotFound) {
			return nil, status.Error(codes.NotFound, domain.ErrOrderNotFound.Error())
		}
		if errors.Is(err, domain.ErrOrderNotOwned) {
			return nil, status.Error(codes.PermissionDenied, domain.ErrOrderNotOwned.Error())
		}
		return nil, status.Error(codes.Internal, "internal server error")
	}
	resp := &orderwritev1.GetOrderDeliveryByIDResponse{
		OrderDelivery:      &orderwritev1.OrderDelivery{},
		OrderDeliveryFiles: make([]*orderwritev1.OrderDeliveryFile, 0, len(res.OrderDeliveryFiles)),
	}
	if res != nil && res.OrderDelivery != nil {
		resp.OrderDelivery = &orderwritev1.OrderDelivery{
			DeliveryMessage: res.OrderDelivery.DeliveryMessage,
			CreatedAt:       res.OrderDelivery.CreatedAt,
		}
	}
	for _, file := range res.OrderDeliveryFiles {
		resp.OrderDeliveryFiles = append(resp.OrderDeliveryFiles, &orderwritev1.OrderDeliveryFile{
			FileId:    file.FileID,
			FileUrl:   file.FileURL,
			SortOrder: file.SortOrder,
			CreatedAt: file.CreatedAt,
		})
	}
	return resp, nil
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

func toRequirementQuestionProto(q *app.OrderRequirementQuestion) *orderwritev1.OrderRequirementQuestion {
	if q == nil {
		return nil
	}
	return &orderwritev1.OrderRequirementQuestion{
		QuestionId: q.QuestionID,
		Text:       q.Text,
		Type:       q.Type,
		Required:   q.Required,
		SortOrder:  q.SortOrder,
	}
}

func toRequirementAnswerProto(a *app.OrderRequirementAnswer) *orderwritev1.OrderRequirementAnswer {
	if a == nil {
		return nil
	}
	return &orderwritev1.OrderRequirementAnswer{Value: a.Value}
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
