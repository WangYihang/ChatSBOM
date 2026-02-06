from sbom_insight.models.framework import Actix
from sbom_insight.models.framework import BaseFramework
from sbom_insight.models.framework import Django
from sbom_insight.models.framework import Echo
from sbom_insight.models.framework import Express
from sbom_insight.models.framework import FastAPI
from sbom_insight.models.framework import Flask
from sbom_insight.models.framework import Framework
from sbom_insight.models.framework import FrameworkFactory
from sbom_insight.models.framework import Gin
from sbom_insight.models.framework import Laravel
from sbom_insight.models.framework import Rails
from sbom_insight.models.framework import SpringBoot
from sbom_insight.models.framework import Symfony


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
