import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

PROJECT = "yourprojectid"
BUCKET = "yourbucketid"
BQ_TABLE = "yourbigQuerytable"
TOPIC = f"projects/{PROJECT}/topics/carpark-data-topic"


class ParseJson(beam.DoFn):
    def process(self, element):
        import json
        record = json.loads(element.decode("utf-8"))

        yield {
            "CarParkID": record.get("CarParkID"),
            "Area": record.get("Area"),
            "Development": record.get("Development"),
            "Location": record.get("Location"),
            "AvailableLots": int(record.get("AvailableLots", 0)),
            "LotType": record.get("LotType"),
            "Agency": record.get("Agency")
        }

def run():
    options = PipelineOptions(
        streaming=True,
        project=PROJECT,
        temp_location=f"gs://{BUCKET}/tmp/",
        region="asia-southeast1"
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=TOPIC)
            | "ParseJSON" >> beam.ParDo(ParseJson())
            | "WriteToBQ" >> beam.io.WriteToBigQuery(
                BQ_TABLE,
                schema={
                    "fields": [
                        {"name": "CarParkID", "type": "STRING"},
                        {"name": "Area", "type": "STRING"},
                        {"name": "Development", "type": "STRING"},
                        {"name": "Location", "type": "STRING"},
                        {"name": "AvailableLots", "type": "INTEGER"},
                        {"name": "LotType", "type": "STRING"},
                        {"name": "Agency", "type": "STRING"}
                    ]
                },
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED",
            )
        )

if __name__ == "__main__":
    run()
