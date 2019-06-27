"""Indigo - Project RADON version - Command line Interface

Copyright 2019 University of Liverpool

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""


__doc_opt__ = """
Indigo Admin Command Line Interface.

Usage:
  iadmin create
  iadmin lu [<name>]
  iadmin lg [<name>]
  iadmin mkuser [<name>]
  iadmin moduser <name> (email | administrator | active | password) [<value>]
  iadmin rmuser [<name>]
  iadmin mkgroup [<name>]
  iadmin atg <name> <userlist> ...
  iadmin rfg <name> <userlist> ...
  iadmin rmgroup [<name>]
  iadmin ingest <user> <group> <path> [--reference --localip <ip> --include <expr> --no-compress]


Options:
  -h --help      Show this screen.
  --version      Show version.
  --reference    Set if we do not want to import the files into Cassandra
  --localip      Specify the IP address for this machine (subnets/private etc)
  --include      include ONLY paths that include this string
  --no-compress  by default compress data when uploaded in Cassandra, set to disable compression
"""

import argparse
import sys
import os
from docopt import docopt
from blessings import Terminal
import logging
from getpass import getpass

import indigo
from indigo import get_config
from indigo.models.errors import GroupConflictError
from indigo.models import (
    Group,
    initialise,
    sync,
    User,
)
from indigo.ingest import do_ingest


class IndigoApplication(object):
    """Methods for the CLI"""


    def __init__(self):
        self.terminal = Terminal()
        initialise()


    def add_to_group(self, args):
        """Add user(s) to a group."""
        groupname = unicode(args['<name>'], "utf-8")
        ls_users = args['<userlist>']
        group = Group.find(groupname)
        if not group:
            self.print_error(u"Group {} doesn't exist".format(groupname))
            return
        added, not_added, already_there = group.add_users(ls_users)
        
        if added:
            self.print_success(u"Added {} to the group {}".format(", ".join(added),
                                                     group.name))
        if already_there:
            if len(already_there) == 1:
                verb = "is"
            else:
                verb = "are"
            self.print_error(u"{} {} already in the group {}".format(
                ", ".join(already_there),
                verb,
                group.name))
        if not_added:
            if len(not_added) == 1:
                msg = u"User {} doesn't exist"
            else:
                msg = u"Users {} don't exist"
            self.print_error(msg.format(", ".join(not_added)))


    def create(self, args):
        """Create the keyspace and the tables"""
        sync()


    def do_ingest(self, args):
        """Ingest a local collection"""
        group_name = unicode(args['<group>'], "utf-8")
        group = Group.find(group_name)
        if not group:
            self.print_error(u"Group {} not found".format(group_name))
            return

        user_name = unicode(args['<user>'], "utf-8")
        user = User.find(user_name)
        if not user:
            self.print_error(u"User {} not found".format(user_name))
            return

        path_name = unicode(args['<path>'], "utf-8")
        path = os.path.abspath(path_name)
        if not os.path.exists(path):
            self.print_error(u"Could not find path {}".format(path_name))
            return
    
        include_pattern = args['--include']
    
        local_ip = args['--localip']
        is_reference = args['--reference']
        compress = not args['--no-compress']
        
        do_ingest(user, group, path)


    def list_groups(self, args):
        """List all groups or a specific group if the name is specified"""
        if args['<name>']:
            name = unicode(args['<name>'], "utf-8")
            group = Group.find(name)
            if group:
                group_info = group.to_dict()
                members = ", ".join(group_info.get("members", []))
                print u"{0.bold}Group name{0.normal}: {1}".format(
                    self.terminal,
                    group_info.get("name", name))
                print u"{0.bold}Group id{0.normal}: {1}".format(
                    self.terminal,
                    group_info.get("uuid", ""))
                print u"{0.bold}Members{0.normal}: {1}".format(
                    self.terminal,
                    members)
            else:
                self.print_error(u"Group {} not found".format(name))
        else:
            for group in Group.objects.all():
                print group.name


    def list_users(self, args):
        """List all users or a specific user if the name is specified"""
        if args['<name>']:
            name = unicode(args['<name>'], "utf-8")
            user = User.find(name)
            if user:
                user_info = user.to_dict()
                groups = u", ".join([el['name']
                                     for el in user_info.get("groups", [])])
                print u"{0.bold}User name{0.normal}: {1}".format(
                    self.terminal,
                    user_info.get("username", name))
                print u"{0.bold}Email{0.normal}: {1}".format(
                    self.terminal,
                    user_info.get("email", ""))
                print u"{0.bold}User id{0.normal}: {1}".format(
                    self.terminal,
                    user_info.get("uuid", ""))
                print u"{0.bold}Administrator{0.normal}: {1}".format(
                    self.terminal,
                    user_info.get("administrator", False))
                print u"{0.bold}Active{0.normal}: {1}".format(
                    self.terminal,
                    user_info.get("active", False))
                print u"{0.bold}Groups{0.normal}: {1}".format(
                    self.terminal,
                    groups)
            else:
                self.print_error(u"User {} not found".format(name))
        else:
            for user in User.objects.all():
                print user.name


    def mk_group(self, args):
        """Create a new group. Ask in the terminal for mandatory fields"""
        if not args['<name>']:
            groupname = raw_input("Please enter the group name: ")
        else:
            groupname = args['<name>']
        groupname = unicode(groupname, "utf-8")

        group = Group.find(groupname)
        if group:
            self.print_error(u"Groupname {} already exists".format(groupname))
            return
        group = Group.create(name=groupname)
        print u"Group {} has been created".format(groupname)


    def mk_user(self, args):
        """Create a new user. Ask in the terminal for mandatory fields"""
        if not args['<name>']:
            username = raw_input("Please enter the user's username: ")
        else:
            username = args['<name>']
        username = unicode(username, "utf-8")
        if User.find(username):
            self.print_error(u"Username {} already exists".format(username))
            return
        admin = raw_input("Is this an administrator? [y/N] ")
        email = ""
        while not email:
            email = raw_input("Please enter the user's email address: ")
        password = ""
        while not password:
            password = getpass("Please enter the user's password: ")
        User.create(name=username,
                password=password,
                email=email,
                administrator=(admin.lower() == 'y'))
        print u"User {} has been created".format(username)


    def mod_user(self, args):
        """Modify a user. Ask in the terminal if the value isn't
        provided"""
        name = unicode(args['<name>'], "utf-8")
        user = User.find(name)
        if not user:
            self.print_error("User {} doesn't exist".format(name))
            return
        value = unicode(args['<value>'], "utf-8")
        if not value:
            if args['password']:
                while not value:
                    value = getpass("Please enter the new password: ")
            else:
                while not value:
                    value = raw_input("Please enter the new value: ")
                value = unicode(args['<value>'], "utf-8")
        if args['email']:
            user.update(email=value)
        elif args['administrator']:
            user.update(administrator=value.lower() in ["true", "y", "yes"])
        elif args['active']:
            user.update(active=value.lower() in ["true", "y", "yes"])
        elif args['password']:
            user.update(password=value)
        print u"User {} has been modified".format(name)


    def print_error(self, msg):
        """Display an error message."""
        print u"{0.bold_red}Error{0.normal} - {1}".format(self.terminal,
                                                          msg)


    def print_success(self, msg):
        """Display a success message."""
        print u"{0.bold_green}Success{0.normal} - {1}".format(self.terminal,
                                                              msg)


    def rm_from_group(self, args):
        """Remove user(s) from a group."""
        groupname = args['<name>']
        groupname = unicode(args['<name>'], "utf-8")
        group = Group.find(groupname)
        if not group:
            self.print_error(u"Group {} doesn't exist".format(groupname))
            return
        ls_users = args['<userlist>']
        removed, not_there, not_exist = group.rm_users(ls_users)
        if removed:
            self.print_success(u"Removed {} from the group {}".format(", ".join(removed),
                                                             group.name))
        if not_there:
            if len(not_there) == 1:
                verb = "isn't"
            else:
                verb = "aren't"
            self.print_error(u"{} {} in the group {}".format(", ".join(not_there),
                                                      verb,
                                                      group.name))
        if not_exist:
            if len(not_exist) == 1:
                msg = u"{} doesn't exist"
            else:
                msg = u"{} don't exist"
            self.print_error(msg.format(", ".join(not_exist)))


    def rm_group(self, args):
        """Remove a group."""
        if not args['<name>']:
            groupname = raw_input("Please enter the group name: ")
        else:
            groupname = args['<name>']
        groupname = unicode(groupname, "utf-8")
        group = Group.find(groupname)
        if not group:
            self.print_error(u"Group {} doesn't exist".format(groupname))
            return
        group.delete()
        print u"Group {} has been deleted".format(groupname)


    def rm_user(self, args):
        """Remove a user."""
        if not args['<name>']:
            username = raw_input("Please enter the user's username: ")
        else:
            username = args['<name>']
        username = unicode(username, "utf-8")
        user = User.find(username)
        if not user:
            self.print_error("User {} doesn't exist".format(username))
            return
        user.delete()
        print u"User {} has been deleted".format(username)


def main():
    """Main function"""
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("models").setLevel(logging.WARNING)
    logging.getLogger("dse.policies").setLevel(logging.WARNING)
    logging.getLogger("dse.cluster").setLevel(logging.WARNING)
    logging.getLogger("dse.cqlengine.management").setLevel(logging.WARNING)
    
    arguments = docopt(__doc_opt__,
                       version='Indigo Admin CLI {}'.format(indigo.__version__))
    app = IndigoApplication()

    if arguments['atg']:
        return app.add_to_group(arguments)
    elif arguments['create']:
        return app.create(arguments)
    elif arguments['ingest']:
        return app.do_ingest(arguments)
    elif arguments['lu']:
        return app.list_users(arguments)
    elif arguments['lg']:
        return app.list_groups(arguments)
    elif arguments['mkgroup']:
        return app.mk_group(arguments)
    elif arguments['mkuser']:
        return app.mk_user(arguments)
    elif arguments['moduser']:
        return app.mod_user(arguments)
    if arguments['rfg']:
        return app.rm_from_group(arguments)
    elif arguments['rmgroup']:
        return app.rm_group(arguments)
    elif arguments['rmuser']:
        return app.rm_user(arguments)


if __name__ == '__main__':
    main()

