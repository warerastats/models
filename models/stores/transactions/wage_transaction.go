package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type WageTransaction struct {
	ID               bson.ObjectID `bson:"id"`
	EmployeeID       bson.ObjectID `bson:"employeeId"`
	EmployerID       bson.ObjectID `bson:"employerId"`
	Money            float64       `bson:"money"`
	ProductionPoints int           `bson:"quantity"`
}

type WageTransactionStore struct {
	coll *mongo.Collection
}

func NewWageTransactionStore(ctx context.Context, db *mongo.Database) *WageTransactionStore {
	store := &WageTransactionStore{
		coll: db.Collection("wage_transactions"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *WageTransactionStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "employeeId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on wage_transactions.employeeId",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "employerId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on wage_transactions.employerId",
			"error", err,
		)
		return
	}
}

func (s *WageTransactionStore) Create(
	ctx context.Context,
	id bson.ObjectID,
	employeeID bson.ObjectID,
	employerID bson.ObjectID,
	money float64,
	productionPoints int,
) error {
	tx := WageTransaction{
		ID:               id,
		EmployeeID:       employeeID,
		EmployerID:       employerID,
		Money:            money,
		ProductionPoints: productionPoints,
	}

	_, err := s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "id", Value: id}},
		bson.D{{Key: "$setOnInsert", Value: tx}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}
