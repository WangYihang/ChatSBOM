from chatsbom.research.models.framework import Actix
from chatsbom.research.models.framework import BaseFramework
from chatsbom.research.models.framework import Chi
from chatsbom.research.models.framework import Django
from chatsbom.research.models.framework import Echo
from chatsbom.research.models.framework import Express
from chatsbom.research.models.framework import FastAPI
from chatsbom.research.models.framework import Flask
from chatsbom.research.models.framework import Framework
from chatsbom.research.models.framework import FrameworkFactory
from chatsbom.research.models.framework import Gin
from chatsbom.research.models.framework import Laravel
from chatsbom.research.models.framework import Rails
from chatsbom.research.models.framework import SpringBoot
from chatsbom.research.models.framework import Symfony


def test_framework_enum_values():
    """Test that all Framework enum values are lowercase strings."""
    assert Framework.GIN == 'gin'
    assert Framework.ECHO == 'echo'
    assert Framework.FASTAPI == 'fastapi'
    assert Framework.FLASK == 'flask'
    assert Framework.DJANGO == 'django'
    assert Framework.SPRINGBOOT == 'springboot'
    assert Framework.RAILS == 'rails'
    assert Framework.LARAVEL == 'laravel'
    assert Framework.SYMFONY == 'symfony'
    assert Framework.ACTIX == 'actix'
    assert Framework.EXPRESS == 'express'


def test_framework_str_repr():
    """Test __str__ and __repr__ return lowercase value."""
    assert str(Framework.GIN) == 'gin'
    assert repr(Framework.DJANGO) == 'django'


def test_framework_factory_create_all():
    """Test FrameworkFactory can create all framework handlers."""
    assert isinstance(FrameworkFactory.create(Framework.GIN), Gin)
    assert isinstance(FrameworkFactory.create(Framework.ECHO), Echo)
    assert isinstance(FrameworkFactory.create(Framework.CHI), Chi)
    assert isinstance(FrameworkFactory.create(Framework.FASTAPI), FastAPI)
    assert isinstance(FrameworkFactory.create(Framework.FLASK), Flask)
    assert isinstance(FrameworkFactory.create(Framework.DJANGO), Django)
    assert isinstance(
        FrameworkFactory.create(
            Framework.SPRINGBOOT,
        ), SpringBoot,
    )
    assert isinstance(FrameworkFactory.create(Framework.RAILS), Rails)
    assert isinstance(FrameworkFactory.create(Framework.LARAVEL), Laravel)
    assert isinstance(FrameworkFactory.create(Framework.SYMFONY), Symfony)
    assert isinstance(FrameworkFactory.create(Framework.ACTIX), Actix)
    assert isinstance(FrameworkFactory.create(Framework.EXPRESS), Express)


def test_all_frameworks_inherit_base():
    """Test all framework classes inherit from BaseFramework."""
    for fw in Framework:
        handler = FrameworkFactory.create(fw)
        assert isinstance(handler, BaseFramework)


def test_all_frameworks_return_package_names():
    """Test all frameworks return non-empty package names list."""
    for fw in Framework:
        handler = FrameworkFactory.create(fw)
        names = handler.get_package_names()
        assert isinstance(names, list)
        assert len(names) > 0
        assert all(isinstance(n, str) for n in names)


def test_gin_package_names():
    """Test Gin returns correct package names."""
    handler = Gin()
    names = handler.get_package_names()
    assert 'github.com/gin-gonic/gin' in names


def test_flask_package_names():
    """Test Flask returns correct package names."""
    handler = Flask()
    names = handler.get_package_names()
    assert 'flask' in names


def test_django_package_names():
    """Test Django returns correct package names."""
    handler = Django()
    names = handler.get_package_names()
    assert 'django' in names


def test_express_package_names():
    """Test Express returns correct package names."""
    handler = Express()
    names = handler.get_package_names()
    assert 'express' in names


def test_laravel_package_names():
    """Test Laravel returns correct package names."""
    handler = Laravel()
    names = handler.get_package_names()
    assert 'laravel/framework' in names


def test_excluded_package_names():
    """Test get_excluded_package_names returns correct list for frameworks."""
    # Base case: most should be empty
    assert Gin().get_excluded_package_names() == []
    assert FastAPI().get_excluded_package_names() == []

    # Flask should exclude FastAPI
    assert 'fastapi' in Flask().get_excluded_package_names()


def test_go_frameworks_list_every_major_version_path():
    """Go modules from v2 on put the major version in the path (echo/v4,
    chi/v5); names match exactly, so a bare path alone misses most users."""
    assert 'github.com/labstack/echo/v4' in Echo().get_package_names()
    assert {'github.com/go-chi/chi', 'github.com/go-chi/chi/v5'} <= set(
        Chi().get_package_names(),
    )


def test_springboot_matches_maven_coordinates():
    """SBOMs name Maven packages group:artifact; the bare artifact id found
    87 Spring Boot web apps where the coordinates find 387."""
    names = SpringBoot().get_package_names()
    assert 'org.springframework.boot:spring-boot-starter-web' in names
    assert 'org.springframework.boot:spring-boot-starter-webflux' in names
    assert 'org.springframework.boot:spring-boot-starter-webmvc' in names
    assert 'spring-boot-starter-web' in names
    openapi = SpringBoot().get_openapi_packages()
    assert 'org.springdoc:springdoc-openapi-starter-webmvc-ui' in openapi
    assert set(SpringBoot().get_generation_commands()) <= set(openapi)
