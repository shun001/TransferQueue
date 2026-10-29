"""Exercise NIXL control sequencing with an in-memory data transport."""

import asyncio
from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest
import zmq

from transfer_queue.storage.payload_transfer.base import PayloadTransferTimeout
from transfer_queue.storage.payload_transfer.nixl import NixlPayloadTransfer, ReceiveToken
from transfer_queue.utils.serial_utils import calc_packed_size, pack_into
from transfer_queue.utils.zmq_utils import ZMQMessage, ZMQRequestType


def completed(value):
    future = Future()
    future.set_result(value)
    return future


@pytest.mark.parametrize("failure", [None, "send", "commit_reply"])
@pytest.mark.parametrize("operation", ["put", "update"])
def test_write_protocol_commits_once_and_cleans_up(failure, operation):
    client = object.__new__(NixlPayloadTransfer)
    server = object.__new__(NixlPayloadTransfer)
    server._pending_puts = {}
    server._runtime = MagicMock()
    payloads = {}
    schema = {"value": {"dtype": None, "shapes": None}}
    update = MagicMock(return_value=schema)
    store = MagicMock()
    sent = []

    server.prepare_receive = lambda descriptor: ReceiveToken({})
    server.receive = lambda descriptor: completed(payloads.pop(descriptor.transfer_id))
    server.cancel_receive = lambda transfer_id: payloads.pop(transfer_id, None)
    client._peer_endpoint = lambda target: None

    def send(endpoint, token, descriptor, frames):
        if failure == "send":
            raise RuntimeError("data transport failed")
        payload = bytearray(calc_packed_size(frames))
        pack_into(payload, frames)
        payloads[descriptor.transfer_id] = memoryview(payload)
        return completed(None)

    client.send = send

    def handle(request):
        return server.handle_request(
            request,
            storage_id="storage",
            load_data=MagicMock(),
            store_data=store,
            update_data=update,
        )

    async def cancel(sender, target, request_type, transfer_id):
        handle(ZMQMessage.create(request_type=request_type, sender_id=sender, body={"transfer_id": transfer_id}))

    client._cancel = cancel

    class Socket:
        async def send_multipart(self, frames, **kwargs):
            request = ZMQMessage.deserialize(frames)
            sent.append(request)
            self.reply = handle(request)

        async def recv_multipart(self, **kwargs):
            if failure == "commit_reply" and len(sent) == 2:
                raise zmq.error.Again()
            return self.reply.serialize()

    options = dict(
        control_socket=Socket(), sender_id="manager", target_id="storage", global_indexes=[0], data={"value": [7]}
    )
    if operation == "update":
        merge = lambda old, new: old + new
        plan = {"value": {"held": [True]}}
        options.update(merge_fn=merge, merge_plan=plan)
    else:
        options["data_parser"] = None
    call = getattr(client, operation)
    if failure is None:
        result = asyncio.run(call(**options))
        if operation == "update":
            assert result[0] > 0
            assert result[1] == schema
        else:
            assert result > 0
    else:
        error = PayloadTransferTimeout if failure == "commit_reply" else RuntimeError
        with pytest.raises(error):
            asyncio.run(call(**options))
    expected_calls = 0 if failure == "send" else 1
    assert update.call_count == (expected_calls if operation == "update" else 0)
    assert store.call_count == (expected_calls if operation == "put" else 0)
    assert "data" not in sent[0].body
    assert not server._pending_puts
    assert not payloads
    if operation == "update" and expected_calls:
        indexes, data, merge_fn, merge_plan = update.call_args.args
        assert indexes == [0] and data == {"value": [7]}
        assert merge_fn(2, 3) == 5 and merge_plan == plan
        # A duplicate commit cannot fold the new values into stored state twice.
        duplicate = handle(sent[1])
        assert duplicate.request_type == ZMQRequestType.PUT_GET_ERROR
        assert update.call_count == 1


def test_update_arrivals_count_prepare_only():
    from transfer_queue.storage.simple_storage import _storage_arrival_op_type

    assert _storage_arrival_op_type(ZMQRequestType.UPDATE_DATA_PREPARE) == "UPDATE_DATA"
    assert _storage_arrival_op_type(ZMQRequestType.UPDATE_DATA_COMMIT) is None
    assert _storage_arrival_op_type(ZMQRequestType.UPDATE_DATA_CANCEL) is None
    assert _storage_arrival_op_type(ZMQRequestType.UPDATE_DATA) == "UPDATE_DATA"
