import grpc
import streaming_pb2
import streaming_pb2_grpc

UPLOAD_FILE_PATH = 'proofs/10mibfile.receipt'
DOWNLOAD_FILE = '10mibfile.receipt'
DOWNLOAD_FILE_PATH = 'proofs/e5b844cc57f57094ea4585e235f36c78c1cd222262bb89d53c94dcb4d6b3e55d.receipt'

def upload_file(stub):
    with open(UPLOAD_FILE_PATH, 'rb') as file:
        def file_chunks():
            while chunk := file.read(4096):
                yield streaming_pb2.FileChunk(chunk=chunk)

        response = stub.UploadFile(file_chunks())
        print(f"Uploaded file and received file name: {response.file_name}")


def download_file(stub, file_name):
    request = streaming_pb2.FileRequest(file_name=file_name)
    response = stub.StreamFile(request)

    with open(DOWNLOAD_FILE_PATH, 'wb') as file:
        for chunk in response:
            file.write(chunk.chunk)

    print(f"Downloaded file saved as: {DOWNLOAD_FILE_PATH}")


def main():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = streaming_pb2_grpc.FileStreamingServiceStub(channel)

        # Upload a file
        print("Uploading file...")
        upload_file(stub)

        # Download the uploaded file
        print("Downloading file...")
        download_file(stub, DOWNLOAD_FILE)


if __name__ == '__main__':
    main()
