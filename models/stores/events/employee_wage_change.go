package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type EmployeeWageChange struct {
	ID        bson.ObjectID `bson:"_id,omitempty"`
	UserID    bson.ObjectID `bson:"userId"`
	CompanyID bson.ObjectID `bson:"companyId"`
	Wage      float64       `bson:"wage"`
}

type EmployeeWageChangeStore struct {
	coll *mongo.Collection
}

func NewEmployeeWageChangeStore(ctx context.Context, db *mongo.Database) *EmployeeWageChangeStore {
	store := &EmployeeWageChangeStore{
		coll: db.Collection("events_employee_wage_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *EmployeeWageChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_employee_wage_change.userId",
			"error", err,
		)
		return
	}
}

func (s *EmployeeWageChangeStore) Set(ctx context.Context, change EmployeeWageChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *EmployeeWageChangeStore) Get(ctx context.Context, userID bson.ObjectID) (*EmployeeWageChange, error) {
	var change EmployeeWageChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "userId", Value: userID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
