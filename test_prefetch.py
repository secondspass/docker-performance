import prefetch

test_json = [ {u'timestamp': u'2017-06-20T00:00:02.814Z', u'http.response.status': 200,
               u'http.request.method': u'GET', u'http.request.duration':
               0.7483296380000001, u'http.request.uri':
               u'v2/a9cb77c4/37773f3b/manifests/406251de', u'host': u'dc118836',
               u'http.request.useragent': u'docker/17.03.1-ce go/go1.7.5
               git-commit/c6d412e kernel/4.4.0-78-generic os/linux arch/amd64
               UpstreamClient(Go-http-client/1.1)', u'http.response.written': 2631,
               u'http.request.remoteaddr': u'6d01dffe', u'id': u'16ea22395e'},
              {u'timestamp': u'2017-06-20T00:01:37.113Z', u'http.response.status': 201,
               u'http.request.method': u'PUT', u'http.request.duration': 1.043830372,
               u'http.request.uri': u'v2/ca64f105/36adcbc6/manifests/817c8a39', u'host':
               u'260f35e1', u'http.request.useragent': u'docker/17.04.0-ce go/go1.7.5
               git-commit/4845c56 kernel/4.4.0-62-generic os/linux arch/amd64
               UpstreamClient(Docker-Client/17.04.0-ce \\(linux\\))',
               u'http.response.written': 524, u'http.request.remoteaddr': u'4fdf6ce6',
               u'id': u'58f4d2d659'},
              {u'timestamp': u'2017-06-20T00:01:37.545Z',
               u'http.response.status': 200, u'http.request.method': u'GET',
               u'http.request.duration': 0.362330732, u'http.request.uri':
               u'v2/ca64f105/36adcbc6/manifests/817c8a39', u'host': u'ae280635',
               u'http.request.useragent': u'docker/17.04.0-ce go/go1.7.5
               git-commit/4845c56 kernel/4.4.0-62-generic os/linux arch/amd64
               UpstreamClient(Docker-Client/17.04.0-ce \\(linux\\))',
               u'http.response.written': 524, u'http.request.remoteaddr': u'4fdf6ce6',
               u'id': u'341e0391fe'},

              {u'timestamp': u'2017-06-20T02:52:07.913Z', u'http.response.status': 200,
              u'http.request.method': u'GET', u'http.request.duration': 0.660501841,
              u'http.request.uri': u'v2/ca64f105/36adcbc6/blobs/b2867db9', u'host':
              u'79634854', u'http.request.useragent': u'docker/1.12.1 go/go1.6.3
              git-commit/23cf638 kernel/3.13.0-68-generic os/linux arch/amd64
              UpstreamClient(python-requests/2.14.1)', u'http.response.written': 1518,
              u'http.request.remoteaddr': u'9ca2770d', u'id': u'adab231d87'}
              
]

def test_extract():
    data = prefetch.extract(test_json)
    assert data == [
        {},
        {},
        {},
        
