package trackers

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Employee struct {
	ID             bson.ObjectID `bson:"id"`
	UserID         bson.ObjectID `bson:"userId"`
	CompanyID      bson.ObjectID `bson:"companyId"`
	EmployerUserID bson.ObjectID `bson:"employerId"`
	Wage           float64       `bson:"wage"`
	Fidelity       int           `bson:"fidelity"`
}

type EmployeeStore struct {
	coll *mongo.Collection
}

func NewEmployeeStore(ctx context.Context, db *mongo.Database) *EmployeeStore {
	store := &EmployeeStore{
		coll: db.Collection("employees"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *EmployeeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on employees.id",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on employees.userId",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "companyId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on employees.companyId",
			"error", err,
		)
		return
	}
}
