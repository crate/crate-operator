# CrateDB Kubernetes Operator
#
# Licensed to Crate.IO GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

import re
from typing import Optional, Tuple, Union, cast

from verlib2.distutils.version import Version


class CrateVersion(Version):
    """Version numbering for CrateDB releases.

    Implements the standard interface for version number classes as
    described above. A CrateDB version consists of two or three
    dot-separated numeric components, with an optional "-SNAPSHOT" tag
    on the end. Alternatively, a nightly version consists of the term "nightly"
    optionally followed by three dot-separated numeric components indicating
    the version, as well as an optional datetime or date component for the day of the
    build in the form ``YYYY-MM-DD-HH-mm`` or ``YYYYMMDD``.

    The following are valid version numbers (shown in the order that
    would be obtained by sorting according to the supplied cmp function):

    - 0.1
    - 3.2
    - 3.2.0
    - nightly-3.2.1
    - nightly-3.2.1-20190708
    - nightly-3.2.1-2019-07-08-01-02
    - 3.2.1-SNAPSHOT
    - 3.2.1
    - 4.0.0-SNAPSHOT
    - nightly

    The following are examples of invalid version numbers:

    - 1
    - 1.2-SNAPSHOT
    - nightly-3.2
    - nightly-3.2.1-201907080200
    - nightly-3.2.1-201907080200-abc9876

    The rationale for this version numbering system will be explained
    in the distutils documentation.
    """

    version: Optional[Tuple[int, int, int]]

    major: Union[int, None]
    minor: Union[int, None]
    hotfix: Union[int, None]

    stable: bool
    snapshot: bool
    nightly: bool

    date: Optional[str] = None
    date_normalized: Optional[str] = None

    #: The regular expression to match a version string. Valid values are e.g.:
    #:
    #: - ``0.1``
    #: - ``0.1.2``
    #: - ``0.1.2-SNAPSHOT``
    #:
    #: Invalid values are, e.g:
    #:
    #: - ``0``
    #: - ``0.1-SNAPSHOT``
    version_re = re.compile(
        r"""
            ^  # Start of the string
            (?P<major>\d+)\.(?P<minor>\d+)  # Every version has a major.minor
            (\.(?P<hotfix>\d+)(?P<snapshot>-SNAPSHOT)?)?  # optionally hotfix
            $  # End of the string
        """,
        re.ASCII + re.VERBOSE,
    )

    #: The regular expression to match a nightly version string. Valid values
    #: are e.g.:
    #:
    #: - ``nightly``
    #: - ``nightly-0.1.2``
    #: - ``nightly-0.1.2-20190708``
    #: - ``nightly-0.1.2-2019-07-08-01-02``
    #:
    #: Invalid values are, e.g:
    #:
    #: - ``nightly-0.1``
    #: - ``nightly-0.1.2-201907080200``
    #: - ``nightly-0.1.2-201907080200-abc0987``

    nightly_version_re = re.compile(
        r"""
            ^
            (?:nightly)
            (?:
                -(?P<has_version>(?P<major>\d+)\.(?P<minor>\d+)\.(?P<hotfix>\d+))
                (?:
                    -(?P<date>\d{8}|\d{4}-\d{2}-\d{2}-\d{2}-\d{2})  # Match either yyyy-MM-dd-HH-mm or yyyymmdd  # noqa
                )?
            )?$
        """,
        re.ASCII + re.VERBOSE,
    )

    def __init__(self, vstring: str):
        self.parse(vstring)

    def __repr__(self):
        return f"{self.__class__.__name__}('{self}')"

    def __str__(self):
        if self.stable:
            return ".".join(map(str, cast(list, self.version)))

        if self.nightly:
            vstring = "nightly"
            if self.version:
                vstring += f"-{self.major}.{self.minor}.{self.hotfix}"
                if self.date:
                    vstring += f"-{self.date}"
            return vstring

        if self.snapshot:
            return f"{self.major}.{self.minor}.{self.hotfix}-SNAPSHOT"

    def _cmp(self, other):
        if isinstance(other, str):
            other = CrateVersion(other)

        if self.version and other.version and self.version != other.version:
            # For stable or snapshot releases, as well as for nightly versions
            # when both have version information, when the numeric versions
            # don't match the prerelease stuff doesn't matter.
            if self.version < other.version:
                return -1
            else:
                return 1

        if self.stable and other.stable:
            return 0
        if self.snapshot and other.snapshot:
            return 0
        if self.nightly and other.nightly:
            if not self.version and not other.version:
                return 0
            if self.version and not other.version:
                return -1
            if not self.version and other.version:
                return 1

            assert self.version
            assert other.version

            if not self.date_normalized and not other.date_normalized:
                return 0
            if self.date_normalized and not other.date_normalized:
                return -1
            if not self.date_normalized and other.date_normalized:
                return 1

            assert self.date_normalized
            assert other.date_normalized

            if self.date_normalized < other.date_normalized:
                return -1
            if self.date_normalized > other.date_normalized:
                return 1

            return 0
        if self.nightly and (other.snapshot or other.stable):
            if self.version:
                return -1
            return 1
        if self.snapshot and other.stable:
            return -1
        if self.stable and (other.snapshot or other.nightly):
            if other.version:
                return 1
            return -1
        if self.snapshot and other.nightly:
            if other.version:
                return 1
            return -1

        assert False, "versions are not comparable"

    def __lshift__(self, other):
        assert isinstance(other, CrateVersion)

        if self.version and other.version:
            return self.version < other.version
        # at least one does not have a version number, thus being a nightly w/o
        # version thus being greater than the other if the other does have a
        # version.
        if not self.version and not other.version:
            return False
        if self.version and not other.version:
            return True

        return False

    def __rshift__(self, other):
        assert isinstance(other, CrateVersion)

        if self.version and other.version:
            return self.version > other.version
        # at least one does not have a version number, thus being a nightly w/o
        # version thus being greater than the other if the other does have a
        # version.
        if not self.version and not other.version:
            return False
        if self.version and not other.version:
            return False

        return True

    def parse(self, vstring):
        if vstring.startswith("nightly"):
            self._parse_nightly(vstring)
        else:
            self._parse_regular(vstring)

    def _parse_regular(self, vstring):
        match = self.version_re.match(vstring)
        if not match:
            raise ValueError("Invalid version number: '%s'" % vstring)

        major, minor, hotfix, snapshot = match.group(
            "major", "minor", "hotfix", "snapshot"
        )

        if hotfix:
            self.version = tuple(map(int, [major, minor, hotfix]))  # type: ignore[assignment]  # noqa: E501
        else:
            self.version = tuple(map(int, [major, minor])) + (0,)  # type: ignore[assignment]  # noqa: E501

        self.stable = False if snapshot else True
        self.nightly = False
        self.snapshot = True if snapshot else False
        if self.version is not None:
            self.major, self.minor, self.hotfix = self.version

    def _parse_nightly(self, vstring):
        match = self.nightly_version_re.match(vstring)
        if not match:
            raise ValueError("Invalid version number: '%s'" % vstring)

        has_version, major, minor, hotfix, date = match.group(
            "has_version", "major", "minor", "hotfix", "date"
        )

        if has_version:
            self.major, self.minor, self.hotfix = int(major), int(minor), int(hotfix)
            self.version = tuple(map(int, [self.major, self.minor, self.hotfix]))  # type: ignore[assignment]  # noqa: E501
        else:
            self.major, self.minor, self.hotfix = None, None, None
            self.version = None
        self.stable = False
        self.snapshot = False
        self.nightly = True

        self.date = date
        if self.date:
            self.date_normalized = date.replace("-", "")[:8]
