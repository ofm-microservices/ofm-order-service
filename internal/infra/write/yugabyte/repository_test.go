package yugabyte

import (
	"errors"
	"testing"
	"time"

	"order-service/internal/infra/write/yugabyte/model"
)

type scanStub struct {
	err  error
	args []any
}

func (s *scanStub) Scan(dest ...any) error {
	s.args = append([]any(nil), dest...)
	if s.err != nil {
		return s.err
	}
	row := model.OrderRow{
		OrderID:               "order-1",
		SagaID:                "saga-1",
		BuyerID:               "buyer-1",
		SellerID:              "seller-1",
		GigID:                 "gig-1",
		PackageID:             "pkg-1",
		Status:                "completed",
		IdempotencyKey:        "idem-1",
		PaymentIntentID:       "pi-1",
		PaymentReleaseID:      "rel-1",
		FailureReason:         "boom",
		DeliveredAt:           time.Unix(100, 0).UTC(),
		CompletedAt:           time.Unix(200, 0).UTC(),
		DisputedAt:            time.Unix(300, 0).UTC(),
		BuyerResponseDeadline: time.Unix(400, 0).UTC(),
		RevisionCountUsed:     2,
		CreatedAt:             time.Unix(500, 0).UTC(),
		UpdatedAt:             time.Unix(600, 0).UTC(),
	}
	for i, dst := range dest {
		switch d := dst.(type) {
		case *string:
			switch i {
			case 0:
				*d = row.OrderID
			case 1:
				*d = row.SagaID
			case 2:
				*d = row.BuyerID
			case 3:
				*d = row.SellerID
			case 4:
				*d = row.GigID
			case 5:
				*d = row.PackageID
			case 6:
				*d = row.Status
			case 7:
				*d = row.IdempotencyKey
			case 8:
				*d = row.PaymentIntentID
			case 9:
				*d = row.PaymentReleaseID
			case 10:
				*d = row.FailureReason
			}
		case *time.Time:
			switch i {
			case 11:
				*d = row.DeliveredAt
			case 12:
				*d = row.CompletedAt
			case 13:
				*d = row.DisputedAt
			case 14:
				*d = row.BuyerResponseDeadline
			case 16:
				*d = row.CreatedAt
			case 17:
				*d = row.UpdatedAt
			}
		case *int32:
			if i == 15 {
				*d = row.RevisionCountUsed
			}
		}
	}
	return nil
}

func TestScanOrder(t *testing.T) {
	scanner := &scanStub{}
	row, err := scanOrder(scanner)
	if err != nil {
		t.Fatalf("scanOrder: %v", err)
	}
	if row.OrderID != "order-1" || row.PaymentReleaseID != "rel-1" || row.RevisionCountUsed != 2 {
		t.Fatalf("row = %#v", row)
	}
}

func TestScanOrderPropagatesError(t *testing.T) {
	want := errors.New("boom")
	_, err := scanOrder(&scanStub{err: want})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestMapRow(t *testing.T) {
	got := mapRow(model.OrderRow{
		OrderID:               "order-1",
		SagaID:                "saga-1",
		BuyerID:               "buyer-1",
		SellerID:              "seller-1",
		GigID:                 "gig-1",
		GigTitle:              "Gig",
		PackageID:             "pkg-1",
		PackageTier:           "Basic",
		PackageDescription:    "desc",
		PackageDeliveryDays:   3,
		PriceCents:            1000,
		Currency:              "usd",
		Status:                "completed",
		IdempotencyKey:        "idem-1",
		PaymentIntentID:       "pi-1",
		PaymentReleaseID:      "rel-1",
		FailureReason:         "boom",
		DeliveredAt:           time.Unix(100, 0).UTC(),
		CompletedAt:           time.Unix(200, 0).UTC(),
		DisputedAt:            time.Unix(300, 0).UTC(),
		BuyerResponseDeadline: time.Unix(400, 0).UTC(),
		RevisionCountUsed:     2,
		CreatedAt:             time.Unix(500, 0).UTC(),
		UpdatedAt:             time.Unix(600, 0).UTC(),
	})
	if got.OrderID != "order-1" || got.GigTitle != "Gig" || got.PaymentReleaseID != "rel-1" {
		t.Fatalf("got = %#v", got)
	}
}
