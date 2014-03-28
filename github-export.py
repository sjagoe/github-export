from __future__ import absolute_import, unicode_literals

import argparse
import datetime
import functools
import hashlib
import json
import os
import sys
import time

from github import Github


def sha1(data):
    digest = hashlib.sha1()
    digest.update(data)
    return digest.digest().encode('hex')


def bucket(destination, sha):
    bucket = os.path.join(destination, sha[:2])
    if os.path.exists(bucket) and not os.path.isdir(bucket):
        raise RuntimeError('Invalid bucket: {0!r}'.format(bucket))
    if not os.path.exists(bucket):
        os.makedirs(bucket)
    location = os.path.join(bucket, sha[2:])
    return location


def check_rate_limit(gh):
    resettime = gh.rate_limiting_resettime
    remaining, _ = gh.rate_limiting
    wait = resettime - time.time()
    if remaining % 100 == 0:
        print 'Requests remaining: {0}; resets in {1:.1f} minutes'.format(
            remaining, wait / 60)
    if remaining <= 50 and wait > 0:
        print '{0}: Waiting for reset time: {1} seconds'.format(
            datetime.datetime.now().isoformat(b' '), wait)
        time.sleep(wait)
        print '{0}: Waking up from sleep'.format(
            datetime.datetime.now().isoformat(b' '))


def rate_limit(fn):
    @functools.wraps(fn)
    def limit(self, *args, **kwargs):
        check_rate_limit(self.gh)
        return fn(self, *args, **kwargs)
    return limit


class PagedItemIterator(object):

    def __init__(self, gh, paginated_list):
        self.gh = gh
        self.paginated_list = paginated_list
        self._list_iter = iter(paginated_list)

    def __iter__(self):
        return self

    def next(self):
        check_rate_limit(self.gh)
        return next(self._list_iter)


class Exporter(object):

    def __init__(self, token, target_directory):
        self.gh = Github(token)
        self.target_directory = target_directory

    def export_repository(self, owner, name):
        full_name = '{0}/{1}'.format(owner, name)
        repo = self.gh.get_repo(full_name)
        repo_directory = os.path.join(self.target_directory, owner, name)
        if not os.path.exists(repo_directory):
            os.makedirs(repo_directory)
        self._dump(repo, repo_directory)
        for issue in PagedItemIterator(self.gh, repo.get_issues()):
            self._export_issue(issue, repo_directory)
        for pull_request in PagedItemIterator(self.gh, repo.get_pulls()):
            self._export_pull_request(pull_request, repo_directory)
        for commit in PagedItemIterator(self.gh, repo.get_commits()):
            self._export_commit(commit, repo_directory)

    def _export_comment(self, comment, destination):
        self._dump(comment, destination)

    def _export_issue(self, issue, destination):
        self._dump(issue, destination)
        for comment in PagedItemIterator(self.gh, issue.get_comments()):
            self._export_comment(comment, destination)

    def _export_pull_request(self, issue, destination):
        self._dump(issue, destination)
        for comment in PagedItemIterator(self.gh, issue.get_comments()):
            self._export_comment(comment, destination)
        for comment in PagedItemIterator(self.gh, issue.get_review_comments()):
            self._export_comment(comment, destination)

    def _export_commit(self, commit, destination):
        for comment in PagedItemIterator(self.gh, commit.get_comments()):
            self._export_comment(comment, destination)

    @rate_limit
    def _dump(self, obj, destination):
        type_ = type(obj).__name__
        json_data = {
            'type': type_,
            'raw_data': obj.raw_data,
            'headers': obj.raw_headers,
        }
        data = json.dumps(json_data)
        sha = sha1(data)
        location = bucket(destination, sha)
        filename = '{0}.{1}'.format(location, type_)
        with open(filename, 'w') as fh:
            fh.write(data)


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--token', help='Auth token')
    parser.add_argument('owner', help='Owner of the repository')
    parser.add_argument('repo', help='Repository name')
    parser.add_argument('destination')
    return parser.parse_args(args)


def main():
    args = parse_args(sys.argv[1:])
    exporter = Exporter(args.token, args.destination)
    exporter.export_repository(args.owner, args.repo)


if __name__ == '__main__':
    main()
