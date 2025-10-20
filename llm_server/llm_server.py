import asyncio
import grpc
import collab_pb2, collab_pb2_grpc

class LLMServicer(collab_pb2_grpc.LLMServiceServicer):
    async def GetLLMAnswer(self, request, context):
        q = request.query or request.context or ""
        answer = f"[LLM suggestion based on query length {len(q)}]: {q[::-1]}"
        return collab_pb2.LLMResponse(request_id=request.request_id, answer=answer)

async def serve():
    server = grpc.aio.server()
    collab_pb2_grpc.add_LLMServiceServicer_to_server(LLMServicer(), server)
    server.add_insecure_port("[::]:50061")
    print("LLM server listening at 0.0.0.0:50061")
    await server.start()
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve())

