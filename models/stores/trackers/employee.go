package trackers

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Employee struct {
	ID                     bson.ObjectID   `bson:"_id"`
	UserID                 bson.ObjectID   `bson:"userId"`
	CompanyID              bson.ObjectID   `bson:"companyId"`
	EmployerID             bson.ObjectID   `bson:"employerId"`
	Wage                   float64         `bson:"wage"`
	Fidelity               int             `bson:"fidelity"`
	JoinedAt               time.Time       `bson:"joinedAt"`
	LastFidelityIncreaseAt time.Time       `bson:"lastFidelityIncreaseAt"`
	LatestObject           json.RawMessage `bson:"raw"`
	RawHash                string          `bson:"rawHash,omitempty"`
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

func (s *EmployeeStore) Get(ctx context.Context, id bson.ObjectID) (*Employee, error) {
	var employee Employee
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&employee)
	if err != nil {
		return nil, err
	}
	return &employee, nil
}

func (s *EmployeeStore) GetByCompany(ctx context.Context, companyID bson.ObjectID) ([]Employee, error) {
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "companyId", Value: companyID}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var employees []Employee
	for cursor.Next(ctx) {
		var employee Employee
		err = cursor.Decode(&employee)
		if err != nil {
			return nil, err
		}

		employees = append(employees, employee)
	}

	err = cursor.Err()
	if err != nil {
		return nil, err
	}

	return employees, nil
}

func (s *EmployeeStore) Upsert(ctx context.Context, id bson.ObjectID, data Employee) error {
	data.ID = id
	hash := hashRaw(data.LatestObject)
	data.RawHash = hash

	skip, err := rawUnchanged(ctx, s.coll, id, hash)
	if err != nil {
		return err
	} else if skip {
		return nil
	}

	_, err = s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		data,
		options.Replace().SetUpsert(true),
	)
	return err
}

func (s *EmployeeStore) Delete(ctx context.Context, id bson.ObjectID) error {
	_, err := s.coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
	return err
}
