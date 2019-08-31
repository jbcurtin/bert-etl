
def test_psql_context():
  from bert import datasource, utils
  with datasource.Postgres.ParseURL('postgres://jbcurtin:@localhost:5432/jbcurtin'):
    cmd: str = 'psql --list'
    result = utils.run_command(cmd)
    assert 'jbcurtin' in result

def lamdba_event_dynamodb_insert():
    return {
        "Records": [
            {
                "eventID": "765452c1970ea1ab5e29316e9d98d092",
                "eventName": "INSERT",
                "eventVersion": "1.1",
                "eventSource": "aws:dynamodb",
                "awsRegion": "us-east-1",
                "dynamodb": {
                    "ApproximateCreationDateTime": 1567186986.0,
                    "Keys": {
                        "identity": {
                            "S": "06b795cc9a32fa7b7ba8b9c67b59f6000181d6116b43f5e74b114a9b6867a301"
                        }
                    },
                    "NewImage": {
                        "datum": {
                            "M": {
                                "bucket": {
                                    "S": "stpubdata"
                                },
                                "bucket_path": {
                                    "S": "tess/public/ffi/s0001/2018/206/1-1/tess2018206195942-s0001-1-1-0120-s_ffic.fits"
                                },
                                "filepath": {
                                    "S": "/tmp/data/tess2018206195942-s0001-1-1-0120-s_ffic.fits"
                                }
                            }
                        },
                        "identity": {
                            "S": "06b795cc9a32fa7b7ba8b9c67b59f6000181d6116b43f5e74b114a9b6867a301"
                        }
                    },
                    "SequenceNumber": "9478200000000004390974509",
                    "SizeBytes": 322,
                    "StreamViewType": "NEW_IMAGE"
                },
                "eventSourceARN": "arn:aws:dynamodb:us-east-1:637663616902:table/8580e044f592cadf03cb893e6befbf1ab5bbd12b/stream/2019-08-28T17:18:26.462"
            }
        ]
    }
