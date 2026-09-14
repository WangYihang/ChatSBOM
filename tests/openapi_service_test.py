"""The pure logic in the project's largest, previously untested module.

openapi_service.py is 553 lines with no tests. The path normaliser and the
spec parser are what every downstream number depends on — the drift
measurement compares normalised (method, path) pairs across releases, so
a normalisation bug silently changes the result rather than failing.
"""
import pytest

from chatsbom.services.openapi_service import OpenApiService


@pytest.fixture(scope='module')
def svc() -> OpenApiService:
    return OpenApiService()


# --- path normalisation ---------------------------------------------------

@pytest.mark.parametrize(
    'raw,expected', [
        ('/users', '/users'),
        ('/Users', '/users'),
        ('  /users  ', '/users'),
        ('users', '/users'),
        ('', '/'),
        ('/users/', '/users'),
        ('/', '/'),
        ('/users?page=2', '/users'),
    ],
)
def test_shape_is_normalised(svc, raw, expected):
    assert svc.normalize_path(raw) == expected


@pytest.mark.parametrize(
    'raw', [
        '/users/{id}',
        '/users/:id',
        '/users/<id>',
        '/users/<int:id>',
        '/users/{userId}',
    ],
)
def test_every_parameter_syntax_collapses_to_the_same_path(svc, raw):
    """Drift compares paths across releases, so `{id}` and `:id` in two
    versions of the same route must not read as a change."""
    assert svc.normalize_path(raw) == '/users/{}'


def test_multiple_parameters_are_each_collapsed(svc):
    assert svc.normalize_path('/users/{id}/posts/{postId}') == \
        '/users/{}/posts/{}'


def test_mixed_parameter_syntaxes_in_one_path(svc):
    assert svc.normalize_path('/a/{id}/b/:slug/c/<x>') == '/a/{}/b/{}/c/{}'


def test_a_trailing_slash_after_a_parameter(svc):
    assert svc.normalize_path('/users/{id}/') == '/users/{}'


# --- spec parsing ---------------------------------------------------------

YAML_SPEC = """
openapi: 3.0.0
info:
  title: Example
paths:
  /users:
    get:
      summary: list
    post:
      summary: create
  /users/{id}:
    get:
      summary: read
    delete:
      summary: remove
"""


def test_yaml_spec_yields_method_path_pairs(svc):
    assert svc.parse_openapi_spec(YAML_SPEC) == {
        ('GET', '/users'),
        ('POST', '/users'),
        ('GET', '/users/{}'),
        ('DELETE', '/users/{}'),
    }


def test_json_spec_yields_the_same_pairs(svc):
    import json
    import yaml
    as_json = json.dumps(yaml.safe_load(YAML_SPEC))
    assert svc.parse_openapi_spec(as_json, is_yaml=False) == \
        svc.parse_openapi_spec(YAML_SPEC)


def test_methods_are_upper_cased(svc):
    pairs = svc.parse_openapi_spec('paths:\n  /x:\n    GeT: {}\n')
    assert pairs == {('GET', '/x')}


def test_non_http_keys_are_not_methods(svc):
    """`parameters` and `summary` sit beside methods in a path item."""
    spec = """
paths:
  /x:
    get: {}
    parameters: []
    summary: a path
    servers: []
"""
    assert svc.parse_openapi_spec(spec) == {('GET', '/x')}


@pytest.mark.parametrize(
    'method', [
        'get', 'post', 'put', 'delete', 'patch', 'options', 'head',
    ],
)
def test_every_standard_method_is_recognised(svc, method):
    spec = f"paths:\n  /x:\n    {method}: {{}}\n"
    assert svc.parse_openapi_spec(spec) == {(method.upper(), '/x')}


@pytest.mark.parametrize(
    'content', [
        '',
        'not: a: valid: yaml:',
        '[]',
        'null',
        'just a string',
        '{oops',
    ],
)
def test_unusable_content_yields_nothing_rather_than_raising(svc, content):
    assert svc.parse_openapi_spec(content) == set()


def test_a_spec_without_paths_yields_nothing(svc):
    assert svc.parse_openapi_spec(
        'openapi: 3.0.0\ninfo:\n  title: x\n',
    ) == set()


def test_a_paths_value_that_is_not_a_mapping_is_ignored(svc):
    assert svc.parse_openapi_spec('paths: [a, b]\n') == set()


def test_a_path_item_that_is_not_a_mapping_is_skipped(svc):
    spec = 'paths:\n  /good:\n    get: {}\n  /bad: not-a-mapping\n'
    assert svc.parse_openapi_spec(spec) == {('GET', '/good')}


def test_json_parsed_as_yaml_still_works(svc):
    """YAML is a superset of JSON, so the flag being wrong is survivable."""
    assert svc.parse_openapi_spec('{"paths": {"/x": {"get": {}}}}') == \
        {('GET', '/x')}


# --- candidate file discovery ---------------------------------------------

def test_a_spec_at_the_repository_root_is_found(svc):
    assert svc.find_openapi_files(['openapi.yaml']) == ['openapi.yaml']


def test_unrelated_files_are_not_matched(svc):
    assert svc.find_openapi_files(['README.md', 'src/main.go']) == []


def test_matching_is_case_insensitive(svc):
    assert svc.find_openapi_files(['OpenAPI.YAML']) == ['OpenAPI.YAML']


@pytest.mark.parametrize(
    'path', [
        'test/openapi.yaml',
        'tests/openapi.yaml',
        'spec/fixtures/openapi.yaml',
        'examples/openapi.yaml',
        'api-test/openapi.yaml',
        'api_test/openapi.yaml',
    ],
)
def test_test_and_fixture_specs_are_excluded(svc, path):
    """A spec under tests/ describes the test suite, not the product."""
    assert svc.find_openapi_files([path]) == []


def test_a_nested_product_spec_is_kept(svc):
    assert svc.find_openapi_files(['api/v1/openapi.yaml']) == \
        ['api/v1/openapi.yaml']


def test_several_specs_are_all_returned(svc):
    found = svc.find_openapi_files([
        'openapi.yaml', 'api/swagger.json', 'tests/openapi.yaml',
    ])
    assert 'openapi.yaml' in found
    assert 'tests/openapi.yaml' not in found


# --- paths and sizes ------------------------------------------------------

def test_version_path_uses_the_tag_when_present(svc):
    path = svc.get_version_path('v1.2.3', 'a' * 40)
    assert 'v1.2.3' in path.parts


def test_version_path_falls_back_to_head(svc):
    assert 'HEAD' in svc.get_version_path(None, 'a' * 40).parts


def test_directory_size_sums_files(svc, tmp_path):
    (tmp_path / 'a').write_bytes(b'x' * 10)
    nested = tmp_path / 'n'
    nested.mkdir()
    (nested / 'b').write_bytes(b'y' * 5)
    assert svc.get_dir_size(tmp_path) == 15


def test_directory_size_of_a_missing_path_is_zero(svc, tmp_path):
    assert svc.get_dir_size(tmp_path / 'absent') == 0
