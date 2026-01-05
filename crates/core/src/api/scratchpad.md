```
api::timeout
api::req::max_body
api::req::raw_body

api::res::header
api::res::headers
api::res::raw_body
```

--------------------

```
api::timeout

api::req::body($req, $kind?: 'json' | 'cbor' | 'flatbuffers' | 'plain' | 'bytes' | 'auto')

api::res::body($res, $kind?: 'json' | 'cbor' | 'flatbuffers' | 'plain' | 'bytes' | 'auto')
api::res::status($res, $status: int)
api::res::header($res, $name: string, $value: string)
api::res::headers($res, $headers: object)
```


notes on kind:
  - json: only accepts or returns json.
  - cbor: only accepts or returns cbor.
  - flatbuffers: only accepts or returns (surrealdb's) flatbuffers
  - plain: 
      takes the request body in a string (not validating headers, lossy).
      when used in a response, the passed value must be a string
  - bytes: 
      takes in the raw request body (not validating headers)
      when used in a response, the passed value must be bytes
  - auto: automatically chooses between either of the above formats based on headers, defaulting to JSON