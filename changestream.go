package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/FGasper/mongo-speedcam/cursor"
	"github.com/FGasper/mongo-speedcam/history"
	"github.com/FGasper/mongo-speedcam/resumetoken"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func _runChangeStream(ctx context.Context, connstr string, interval time.Duration) error {
	client, err := getClient(connstr)
	if err != nil {
		return err
	}

	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("opening session: %w", err)
	}

	sctx := mongo.NewSessionContext(ctx, sess)

	unixTimeStart := uint32(time.Now().Add(-interval).Unix())
	startTS := bson.Timestamp{T: unixTimeStart}

	db := client.Database("admin")

	fmt.Printf("Gathering change events from the past %s …\n", interval)

	startTime := time.Now()

	pipeline := mongo.Pipeline{
		// 1. $changeStream stage
		{
			{"$changeStream", bson.D{
				{"allChangesForCluster", true},
				{"fullDocument", "default"},
				{"showRawUpdateDescription", true},
				{"showExpandedEvents", true},
				{"showSystemEvents", true},
				{"startAtOperationTime", startTS},
			}},
		},
		// 2. $project stage to exclude fields
		{
			{"$project", bson.D{
				{"lsid", 0},
				{"txnNumber", 0},
				{"wallTime", 0},
				{"updateDescription", 0},
				{"fullDocument", 0},
				{"rawUpdateDescription", 0},
			}},
		},
		// 3. $match stage for filtering
		{
			{"$match", bson.D{
				{"$expr", bson.D{
					{"$and", bson.A{
						// First $not block
						bson.D{
							{"$not", bson.D{
								{"$or", bson.A{
									// $in condition for "$ns.db"
									bson.D{
										{"$in", bson.A{
											"$ns.db",
											bson.A{
												"mongosync_reserved_for_internal_use",
												"admin",
												"local",
												"config",
											},
										}},
									},
									// $eq condition using $indexOfCP
									bson.D{
										{"$eq", bson.A{
											0,
											bson.D{
												{"$indexOfCP", bson.A{
													"$ns.db",
													"mongosync_reserved_for_verification_",
													0,
													1,
												}},
											},
										}},
									},
								}},
							}},
						},
						// Second $not block (for $ns.coll)
						bson.D{
							{"$not", bson.D{
								{"$eq", bson.A{
									0,
									bson.D{
										{"$indexOfCP", bson.A{
											"$ns.coll",
											"system.",
											0,
											1,
										}},
									},
								}},
							}},
						},
					}},
				}},
			}},
		},
		// 4. $addFields stage
		{
			{"$addFields", bson.D{
				{"_msh", bson.D{
					{"$toHashedIndexKey", bson.D{
						{"$_internalKeyStringValue", bson.D{
							{"input", "$documentKey._id"},
						}},
					}},
				}},
			}},
		},
		// 5. $changeStreamSplitLargeEvent stage
		{
			{"$changeStreamSplitLargeEvent", bson.D{}},
		},
	}
	mongosyncQuery := bson.D{
		{"aggregate", 1},
		{"cursor", bson.D{}},
		{"pipeline", pipeline},
	}

	resp := db.RunCommand(
		sctx,
		mongosyncQuery,
	)

	cursor, err := cursor.New(db, resp)
	if err != nil {
		return fmt.Errorf("opening change stream: %w", err)
	}

	eventSizesByType := map[string]int{}
	eventCountsByType := map[string]int{}

	fullEventName := map[string]string{}
	for _, eventName := range eventsToTruncate {
		fullEventName[eventName[:1]] = eventName
	}

	var minUnixSecs, _ uint32

cursorLoop:
	for {
		if cursor.IsFinished() {
			return fmt.Errorf("unexpected end of change stream")
		}

		for _, event := range cursor.GetCurrentBatch() {
			t, _ := event.Lookup("clusterTime").Timestamp()

			if time.Unix(int64(t), 0).After(startTime) {
				break cursorLoop
			}

			if minUnixSecs == 0 {
				minUnixSecs = t
			}

			//maxUnixSecs = t

			//op := event.Lookup("op").StringValue()
			op := "null"

			if fullOp, isShortened := fullEventName[op]; isShortened {
				op = fullOp
			}

			eventCountsByType[op]++
			eventSizesByType[op] += 1
		}

		rt, hasToken := cursor.GetCursorExtra()["postBatchResumeToken"]
		if !hasToken {
			return fmt.Errorf("change stream lacks resume token??")
		}

		tokenTS, err := resumetoken.New(rt.Document()).Timestamp()
		if err != nil {
			return fmt.Errorf("parsing timestamp from change stream resume token")
		}

		if time.Unix(int64(tokenTS.T), 0).After(startTime) {
			break cursorLoop
		}

		if err := cursor.GetNext(sctx); err != nil {
			return fmt.Errorf("iterating change stream: %w", err)
		}
	}

	//delta := time.Duration(1+maxUnixSecs-minUnixSecs) * time.Second

	displayTable(eventCountsByType, eventSizesByType, time.Since(startTime))

	return nil
}

func _runChangeStreamLoop(
	ctx context.Context,
	connstr string,
	window, reportInterval time.Duration,
) error {
	client, err := getClient(connstr)
	if err != nil {
		return err
	}

	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("opening session: %w", err)
	}

	sctx := mongo.NewSessionContext(ctx, sess)

	cs, err := client.Watch(
		sctx,
		mongo.Pipeline{
			{
				{"$project", bson.D{
					{"lsid", 0},
					{"txnNumber", 0},
					{"wallTime", 0},
					{"updateDescription", 0},
					{"fullDocument", 0},
					{"rawUpdateDescription", 0},
				}},
			},
			// 3. $match stage for filtering
			{
				{"$match", bson.D{
					{"$expr", bson.D{
						{"$and", bson.A{
							// First $not block
							bson.D{
								{"$not", bson.D{
									{"$or", bson.A{
										// $in condition for "$ns.db"
										bson.D{
											{"$in", bson.A{
												"$ns.db",
												bson.A{
													"mongosync_reserved_for_internal_use",
													"admin",
													"local",
													"config",
												},
											}},
										},
										// $eq condition using $indexOfCP
										bson.D{
											{"$eq", bson.A{
												0,
												bson.D{
													{"$indexOfCP", bson.A{
														"$ns.db",
														"mongosync_reserved_for_verification_",
														0,
														1,
													}},
												},
											}},
										},
									}},
								}},
							},
							// Second $not block (for $ns.coll)
							bson.D{
								{"$not", bson.D{
									{"$eq", bson.A{
										0,
										bson.D{
											{"$indexOfCP", bson.A{
												"$ns.coll",
												"system.",
												0,
												1,
											}},
										},
									}},
								}},
							},
						}},
					}},
				}},
			},
			// 4. $addFields stage
			{
				{"$addFields", bson.D{
					{"_msh", bson.D{
						{"$toHashedIndexKey", bson.D{
							{"$_internalKeyStringValue", bson.D{
								{"input", "$documentKey._id"},
							}},
						}},
					}},
				}},
			},
			{
				{"$match", bson.D{
					{"$expr", bson.D{
						{"$eq", bson.A{
							bson.D{{"$mod", bson.A{"$_msh", 2}}},
							0,
						}},
					}},
				}},
			},
			// 5. $changeStreamSplitLargeEvent stage
			{
				{"$changeStreamSplitLargeEvent", bson.D{}},
			},
		},
		options.ChangeStream().
			SetCustomPipeline(bson.M{
				"showSystemEvents":         true,
				"showExpandedEvents":       true,
				"showRawUpdateDescription": true,
			}),
	)
	if err != nil {
		return fmt.Errorf("opening change stream: %w", err)
	}
	defer cs.Close(sctx)

	fmt.Printf("Listening for change events. Stats showing every %s …\n", reportInterval)

	eventsHistory := history.New[eventStats](window)

	var changeStreamLag atomic.Pointer[time.Duration]

	go func() {
		for {
			time.Sleep(reportInterval)

			totalStats, _, curStatsInterval := tallyEventsHistory(eventsHistory)

			displayTable(totalStats.counts, totalStats.sizes, curStatsInterval)

			fmt.Printf("Change stream lag: %s\n", lo.FromPtr(changeStreamLag.Load()))
		}
	}()

	fullEventName := map[string]string{}
	for _, eventName := range eventsToTruncate {
		fullEventName[eventName[:1]] = eventName
	}

	var curEventStats eventStats
	initMap(&curEventStats.counts)
	initMap(&curEventStats.sizes)

	for cs.Next(sctx) {
		//op := cs.Current.Lookup("op").StringValue()
		op := "null"

		//if fullOp, isShortened := fullEventName[op]; isShortened {
		//	op = fullOp
		//}

		curEventStats.counts[op]++
		curEventStats.sizes[op] += 1

		if cs.RemainingBatchLength() == 0 {
			eventsHistory.Add(curEventStats)
			initMap(&curEventStats.counts)
			initMap(&curEventStats.sizes)
		}

		sessTS, err := GetClusterTimeFromSession(sess)
		if err != nil {

		} else {
			eventT, _ := cs.Current.Lookup("clusterTime").Timestamp()

			lagSecs := int64(sessTS.T) - int64(eventT)
			changeStreamLag.Store(lo.ToPtr(time.Duration(lagSecs) * time.Second))
		}
	}
	if cs.Err() != nil {
		return fmt.Errorf("reading change stream: %w", cs.Err())
	}

	return fmt.Errorf("unexpected end of change stream")
}
