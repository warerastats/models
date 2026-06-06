package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// UserFinanceReport is a user's per-24h income, spending, and equipment activity.
type UserFinanceReport struct {
	ID              string        `bson:"_id"`
	UserID          bson.ObjectID `bson:"userId"`
	DayStart        time.Time     `bson:"dayStart"`
	WagesPaid       float64       `bson:"wagesPaid"`
	WagesEarned     float64       `bson:"wagesEarned"`
	ItemsBought     float64       `bson:"itemsBought"`
	ItemsSold       float64       `bson:"itemsSold"`
	EquipBought     float64       `bson:"equipBought"`
	EquipSold       float64       `bson:"equipSold"`
	ValueDismantled float64       `bson:"valueDismantled"`
	CasesOpened     int           `bson:"casesOpened"`
	CasesNet        float64       `bson:"casesNet"`
}

// UserFinanceReportID is the deterministic per-user-per-day key.
func UserFinanceReportID(userID bson.ObjectID, dayStart time.Time) string {
	return userID.Hex() + "@" + dayStart.UTC().Format("2006-01-02")
}

type UserFinanceReportStore struct {
	coll *mongo.Collection
}

func NewUserFinanceReportStore(ctx context.Context, db *mongo.Database) *UserFinanceReportStore {
	store := &UserFinanceReportStore{coll: db.Collection("user_finance_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *UserFinanceReportStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "userId", Value: 1}, {Key: "dayStart", Value: -1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on user_finance_reports.{userId,dayStart}", "error", err)
	}
}

// Upsert replaces a batch of per-user daily finance reports keyed on _id.
func (s *UserFinanceReportStore) Upsert(ctx context.Context, rows []UserFinanceReport) error {
	if len(rows) == 0 {
		return nil
	}
	ops := make([]mongo.WriteModel, len(rows))
	for i := range rows {
		ops[i] = mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: rows[i].ID}}).
			SetReplacement(rows[i]).
			SetUpsert(true)
	}
	_, err := s.coll.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}

// GetByDay returns all user finance reports for a given day start.
func (s *UserFinanceReportStore) GetByDay(ctx context.Context, dayStart time.Time) ([]UserFinanceReport, error) {
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "dayStart", Value: dayStart}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []UserFinanceReport
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
