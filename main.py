import grpc
from concurrent import futures
import os
import hashlib
import sqlite3
from grpc import ServicerContext
from typing import Iterator

import streaming_pb2
import streaming_pb2_grpc

DB_PATH = 'file_streaming.db'
UPLOAD_DIR = 'proofs/'

class FileStreamingServiceServicer(streaming_pb2_grpc.FileStreamingServiceServicer):

    def __init__(self):
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        self.init_db()

    def init_db(self):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS files (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            file_name TEXT NOT NULL
                        )''')
        conn.commit()
        conn.close()

    def StreamFile(self, request: streaming_pb2.FileRequest, context: ServicerContext) -> Iterator[streaming_pb2.FileChunk]:
        file_path = os.path.join(UPLOAD_DIR, request.file_name)

        if not os.path.exists(file_path):
            context.abort(grpc.StatusCode.NOT_FOUND, 'File not found.')
        print("Streaming file:", file_path)

        with open(file_path, 'rb') as file:
            while chunk := file.read(4096):
                yield streaming_pb2.FileChunk(chunk=chunk)

    def UploadFile(self, request_iterator: Iterator[streaming_pb2.FileChunk], context: ServicerContext) -> streaming_pb2.FileRequest:
        file_data = bytearray()
        for chunk in request_iterator:
            file_data.extend(chunk.chunk)
        hash_hex = hashlib.sha256(file_data).hexdigest()
        file_name = f"{hash_hex}.receipt"
        file_path = os.path.join(UPLOAD_DIR, file_name)

        # Save file
        with open(file_path, 'wb') as file:
            file.write(file_data)

        # Insert file name in database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('INSERT INTO files (file_name) VALUES (?)', (file_name,))
        conn.commit()
        conn.close()

        return streaming_pb2.FileRequest(file_name=file_name)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    streaming_pb2_grpc.add_FileStreamingServiceServicer_to_server(
        FileStreamingServiceServicer(), server)
    server.add_insecure_port('[::]:50052')
    print("Server is running on port 50052...")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
