import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4

from pydantic import BaseModel
from rsocket.extensions.helpers import composite, route
from rsocket.payload import Payload
from rsocket.transports.aiohttp_websocket import websocket_client


class EventMetadata(BaseModel):
    traceId: str
    spanId: str


class SourceSystem(BaseModel):
    name: str


class NotificationEventType(BaseModel):
    eventType: str


class EventSource(BaseModel):
    correlationId: str = str(uuid4())
    createdAt: str = datetime.now(timezone.utc).isoformat()
    senderId: str = None
    type: NotificationEventType
    sourceSystem: SourceSystem
    data: Dict[str, Any]
    metadata: EventMetadata

    def to_json(self):
        return json.dumps(self.dict())


async def send_event_to_rsocket():
    # Establish a connection to the RSocket server
    async with websocket_client('http://localhost:%s/rsocket' % 8888) as client:
        # Create an EventSource object and serialize it to a JSON string
        event = EventSource(
            correlationId=str(uuid4()),
            type=NotificationEventType(eventType="event_type"),
            sourceSystem=SourceSystem(name="source_system"),
            data={"key": "value"},
            metadata=EventMetadata(traceId="trace_id", spanId="span_id")
        )
        json_data = event.to_json()

        # Send a request to the server with the JSON data as the payload
        payload = Payload(json_data.encode("utf-8"), composite(route('notification-rr')))
        response = await client.request_response(payload)
        print(response.data.decode("utf-8"))

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    asyncio.run(send_event_to_rsocket())

    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
