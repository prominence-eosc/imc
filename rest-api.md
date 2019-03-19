# REST API

This is the specification of the IMC REST API. This is still considered a work in progress, so things could change or break with every update.

## Infrastructure API

The Infrastructure API exposes endpoints for managing infrastructure. Note that the API is asynchronous, i.e. it immediately gives a response but the action is carried out in the background.

### Describe infrastructure

```
GET /v1/infrastructures/<id>
```

Get information about the specified infrastructure.

#### Example Request

```http
GET /v1/infrastructures/b4486ddb-0bb5-4056-b74a-5adf49928eb6 HTTP/1.1
```

#### Example Response

```http
HTTP/1.1 200 OK
Content-Type: application/json
```

### Query Parameters

None.

#### Status Codes

- **200** - no error
- **401** - unauthorized
- **404** - not found

### Deploy infrastructure

```
POST /v1/infrastructures
Content-Type: application/json
```
Deploy infrastructure.

#### Example Request

#### Example Response

```http
HTTP/1.1 201 Created
Content-Type: application/json
```

```json
{
  "id": 1
}
```
#### Request Body

The following fields are used in the request body for creating a job:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `image` | `string` | Yes | Container image name. |
| `cmd` | `string` | No | Command to run inside the container. |
| `args` | `string` | No | Arguments of command run inside the container. |
| `nodes` | `integer` | No | Number of nodes required. |
| `cpus` | `integer` | No | Number of CPU cores per node required. |
| `memory` | `integer` | No | Memory per node required in GB. |
| `disk` | `integer` | No | Disk space required in GB. |
| `runtime` | `integer` | No | Maximum runtime of the job in minutes. |
| `inputs` | | No | List of filenames and their base64-encoded content to be made available to jobs. |
| `outputFiles` | `array[string]` | No | List of output filenames to be uploaded to storage. |
| `outputDirs` | `array[string]` | No | List of output directories to be uploaded to storage. |
| `artifacts` | `array[string]` | No | List of URLs to fetch before the job starts. |
| `env` | `array[string]` | No | List of environment variables in the form of name-value pairs, e.g. `name=value`. |
| `labels` | `array[string]` | No | List of arbitrary labels in the form of name-value pairs, e.g. `name=value`. |
| `type` | `string` | No | Type of job. By default `basic` is used. For an MPI job use `mpi`. |
| `instances` | `integer` | No | Number of instances of this job. |
| `parallelism` | `integer` | No | Maximum number of idle and running instances of this job. |

#### Status Codes

- **201** - no error
- **400** - bad request
- **401** - unauthorized

### Delete infrastructure

```
DELETE /v1/infrastructures/<id>
```

Delete the specified infrastructure.

#### Example Request

```http
DELETE /v1/infrastructures/b4486ddb-0bb5-4056-b74a-5adf49928eb6 HTTP/1.1
```

#### Example Response

#### Status Codes

- **200** - no error
- **400** - bad request
- **401** - unauthorized
- **404** - not found
