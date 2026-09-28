"""What a CocoaPods podspec declares (#55 pilot).

jasnig/ZJScrollPageView's only manifest is `ZJScrollPageView.podspec`,
and discovery did not know the name: Syft reads only `Podfile.lock`, the
dependency graph has no CocoaPods, and a pod's own dependencies were in
no source.
"""
import json

from chatsbom.core.podspec import declarations
from chatsbom.core.podspec import Pod
from chatsbom.core.podspec import read

#: jasnig/ZJScrollPageView at 2a75594, as it is: no dependency at all.
ZJ_SCROLL_PAGE_VIEW = '''
Pod::Spec.new do |s|
  s.name        = "ZJScrollPageView"
  s.version     = "1.0.0"
  s.license     = { :type => "MIT" }
  s.platform     = :ios, "8.0"
  s.source   = { :git => "https://github.com/jasnig/ZJScrollPageView.git", :tag => s.version }
  s.framework  = "UIKit"
  s.source_files = "ZJScrollPageView/ZJScrollPageView/*.h","ZJScrollPageView/ZJScrollPageView/*.m"
end
'''

WITH_DEPENDENCIES = '''
Pod::Spec.new do |spec|
  spec.name = 'Kingfisher'
  spec.dependency 'Alamofire', '~> 5.0'
  spec.dependency "SnapKit"
  spec.dependency 'RxSwift', '>= 6.0', '< 7.0'   # a range
  # spec.dependency 'Commented', '1.0'
  spec.subspec 'Core' do |ss|
    ss.dependency 'AFNetworking/NSURLSession', '~> 4.0'
    ss.dependency 'Kingfisher/Base'
  end
end
'''


def test_a_spec_with_no_dependency_declares_none():
    assert read('ZJScrollPageView.podspec', ZJ_SCROLL_PAGE_VIEW) == []


def test_every_dependency_with_its_requirement():
    assert read('Kingfisher.podspec', WITH_DEPENDENCIES) == [
        Pod('Alamofire', '~> 5.0'),
        Pod('SnapKit', ''),
        Pod('RxSwift', '>= 6.0, < 7.0'),
        # A subspec of another pod is that pod; one of its own is not a
        # dependency.
        Pod('AFNetworking', '~> 4.0'),
    ]


def test_the_json_form_and_its_subspecs():
    spec = {
        'name': 'Foo',
        'dependencies': {'Bar': ['~> 1.2'], 'Baz': []},
        'subspecs': [
            {'name': 'Core', 'dependencies': {'Qux': '2.0', 'Foo/Util': []}},
        ],
    }
    assert read('Foo.podspec.json', json.dumps(spec)) == [
        Pod('Bar', '~> 1.2'), Pod('Baz', ''), Pod('Qux', '2.0'),
    ]


def test_unreadable_json_declares_nothing():
    assert read('Foo.podspec.json', '{not json') == []


def test_purls_are_cocoapods_purls():
    assert Pod('Alamofire', '~> 5.0').purl == 'pkg:cocoapods/Alamofire@~%3E%205.0'
    assert Pod('SnapKit').purl == 'pkg:cocoapods/SnapKit'


def test_only_podspecs_are_read():
    files = [
        ('Podfile', "pod 'Alamofire'\n"),
        ('Kingfisher.podspec', WITH_DEPENDENCIES),
        ('Broken.podspec', None),
    ]
    assert [(p, pod.name) for p, pod in declarations(files)] == [
        ('Kingfisher.podspec', 'Alamofire'),
        ('Kingfisher.podspec', 'SnapKit'),
        ('Kingfisher.podspec', 'RxSwift'),
        ('Kingfisher.podspec', 'AFNetworking'),
    ]
