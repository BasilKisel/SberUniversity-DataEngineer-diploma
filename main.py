#!/usr/bin/python3
# encoding=utf8

import argparse
import sys
import traceback
from pathlib import Path
import jaydebeapi as jdba
from py_scripts.workers import ResourceType
from py_scripts.workers import Metadata
from py_scripts.workers import get_supported_etl 
from py_scripts.workers import get_supported_reports
from py_scripts.workers import EtlPhase

#####################################################################
### Task executors                                                ###
#####################################################################

class ResourceManager:
    """Controls resource management and task processing."""

    def init_sql_wrapper(self, wrapper):
        wrapper.init_sql(self._get_resource(ResourceType.SQL_SCRIPT_DIR))
        return wrapper

    def _get_resource(self, res_type):
        if (res_type == ResourceType.FILE_SHARE):
            return Path('.')
        elif (res_type == ResourceType.ARCHIVE_SHARE):
            return Path('.', 'archive')
        elif (res_type == ResourceType.SQL_SCRIPT_DIR):
            return Path('.', 'sql_scripts')
        elif (res_type == ResourceType.BANK_OLTP or res_type == ResourceType.TARGET_DWH):
            de1m_cred = {'user': 'de1m', 'password': 'samwisegamgee'}
            orajdbc_paths = (
                    Path(r'OJDBC8-Full', r'ojdbc8.jar'),
                    Path(r'..', r'ojdbc8.jar'),
                )
            path_to_ojdbc = None
            for jdbc_path in orajdbc_paths:
                if jdbc_path.is_file() and jdbc_path.exists():
                    path_to_ojdbc = str(jdbc_path)
                    break
            if not path_to_ojdbc:
                raise Exception('ERROR: cannot find ojdbc driver in paths: "{}"'.format(orajdbc_paths))

            conn = None
            try:
                conn = jdba.connect(
                        r'oracle.jdbc.driver.OracleDriver',
                        r'jdbc:oracle:thin:@de-oracle.chronosavant.ru:1521:deoracle',
                        de1m_cred,
                        path_to_ojdbc
                )
                # https://docs.oracle.com/en/database/oracle/oracle-database/12.2/jajdb/
                # Oracle OracleDriver class doesn't have autocommit property :(
                # Thanks for the tip to https://gist.github.com/duckpunch/1953804
                conn.jconn.setAutoCommit(False)
                return conn
            except Exception as ex:
                print('ERROR: {}'.format(ex))
                traceback.print_exc()
                if conn:
                    try:
                        conn.close()
                    except Exception as ex:
                        print('ERROR: {}'.format(ex))
                        traceback.print_exc()
        else:
            raise Exception('Unsupported resource type.')

    def _try_do_for_all(self, objs, action):
        """ Helper to manage objects' state such as DB connections."""
        for obj in objs:
            try:
                action(obj)
            except Exception as ex:
                print('ERROR: {}'.format(ex))
                traceback.print_exc()

    def perform(self, worker, phase, meta):
        """Allocate resources and process the phase"""
        try:
            file_res = {res_type: self._get_resource(res_type) for res_type in worker.get_file_resource_types(phase)}
            db_conns = {res_type: self._get_resource(res_type) for res_type in worker.get_db_resource_types(phase)}
            db_curs = {res_type: db_conn.cursor() for res_type, db_conn in db_conns.items()}
            resources = dict()
            resources.update(file_res)
            resources.update(db_curs)
            status_code = worker.perform(phase, resources, meta)
            self._try_do_for_all(db_conns.values(), lambda conn: conn.commit())
            return status_code
        except Exception as ex:
            print('ERROR: {}'.format(ex))
            traceback.print_exc()
            if db_conns:
                self._try_do_for_all(db_conns.values(), lambda conn: conn.rollback())
            return None
        finally:
            if db_conns:
                self._try_do_for_all(db_conns.values(), lambda conn: conn.close())


#####################################################################
### Functions for argparse commands                               ###
#####################################################################

def start_etl(args):
    print('INFO: started "{}"'.format(args.etl_name))
    meta = args.meta
    res_man = args.resource_manager
    try:
        # load stage
        loader = res_man.init_sql_wrapper(args.supported_etl[args.etl_name])
        status_code = res_man.perform(loader, EtlPhase.LOAD_STAGE, meta)
        if (status_code != 0):
            print('WARNING: DWH loading phase skipped due to the stage loading was not complete with status code = "{}"'.format(status_code))
            return status_code

        status_code = res_man.perform(loader, EtlPhase.LOAD_DWH, meta)
        if (status_code != 0):
            print('WARNING: cleaning phase skipped due to the DWH loading was not complete with status code = "{}"'.format(status_code))
            return status_code

        status_code = res_man.perform(loader, EtlPhase.CLEANUP, meta)
        if (status_code != 0):
            print('WARNING: clean up phase was not complete with status code = "{}"'.format(status_code))
            return status_code
        
        return status_code
    finally:
        print('INFO: finished "{}"'.format(args.etl_name))


def start_all_etl(args):
    for etl_name in args.supported_etl.keys():
        args.etl_name = etl_name
        start_etl(args)


def build_report(args):
    print('INFO: started "{}"'.format(args.report_name))
    meta = args.meta
    res_man = args.resource_manager
    builder = res_man.init_sql_wrapper(args.supported_reports[args.report_name])
    status_code = res_man.perform(builder, EtlPhase.BUILD_REPORT, meta)
    if (status_code != 0):
        print('WARNING: report "{}" building failed with status code = "{}"'.format(args.report_name, status_code))
    print('INFO: finished "{}"'.format(args.report_name))
    return status_code


def build_all_reports(args):
    for report_name in args.supported_reports.keys():
        args.report_name = report_name
        build_report(args)


### TODO SOMEDAY
#def print_all_meta(args):
#    raise NotImplementedError()
#
#
#def print_meta(args):
#    raise NotImplementedError()
#
#
#def drop_all_meta(args):
#    raise NotImplementedError()
#
#
#def drop_meta(args):
#    raise NotImplementedError()


def append_cmd_subparsers(
        cmd_subparsers,
        supported_objects,
        append_all,
        subcmd_name,
        subcmd_aliases,
        subcmd_title,
        subcmd_dest,
        subcmd_description,
        process_all_func,
        process_obj_func
        ):
    subcmd_parser = cmd_subparsers.add_parser(subcmd_name, aliases = subcmd_aliases)
    obj_subparsers = subcmd_parser.add_subparsers(
            title = subcmd_title,
            dest = subcmd_dest,
            #required = True,
            description = subcmd_description
            )
    if (append_all):
        all_obj_parser = obj_subparsers.add_parser('all')
        all_obj_parser.add_argument(
                '-e',
                '--exclude',
                #action = 'extend',
                action = 'append',
                choices = supported_objects,
                nargs = '+',
                default = []
        )
        all_obj_parser.set_defaults(exec_cmd_func = process_all_func)
    for obj in supported_objects:
        obj_parser = obj_subparsers.add_parser(obj)
        obj_parser.set_defaults(exec_cmd_func = process_obj_func)


#####################################################################
### Menu construction and command linkage                         ###
#####################################################################

def main(supported_etl, supported_reports):
    main_parser = argparse.ArgumentParser(
            description = 'Perform ETL related tasks on banking data.'
    )
    res_man = ResourceManager()
    meta = res_man.init_sql_wrapper(Metadata())
    main_parser.set_defaults(
            supported_etl = supported_etl,
            supported_reports = supported_reports,
            meta = meta,
            resource_manager = res_man
    )
    #main_parser.set_defaults(exec_cmd_func = do_nothing)
    cmd_subparsers = main_parser.add_subparsers(dest = 'subcmd', description = 'Subcommand to start.')
    # START ETL SUBCOMMAND
    append_cmd_subparsers(
        cmd_subparsers = cmd_subparsers,
        supported_objects = supported_etl.keys(),
        append_all = True,
        subcmd_name = 'start_etl',
        subcmd_aliases = ['s'],
        subcmd_title = 'ETL name',
        subcmd_dest = 'etl_name',
        subcmd_description = 'ETL to start.',
        process_all_func = start_all_etl,
        process_obj_func = start_etl
    )
    # BUILD REPORTS SUBCOMMAND
    append_cmd_subparsers(
        cmd_subparsers = cmd_subparsers,
        supported_objects = supported_reports.keys(),
        append_all = True,
        subcmd_name = 'build_report',
        subcmd_aliases = ['b'],
        subcmd_title = 'report name',
        subcmd_dest = 'report_name',
        subcmd_description = 'Report to build.',
        process_all_func = build_all_reports,
        process_obj_func = build_report
    )

### TODO SOMEDAY
#    # METADATA SUBCOMMANDS
#    meta_parser = cmd_subparsers.add_parser('meta', aliases = ['m'])
#    meta_subparsers = meta_parser.add_subparsers(dest = 'meta_type', title = 'type of metadata')
#    etl_meta_parser = meta_subparsers.add_parser('etl', aliases = ['e'])
#    rep_meta_parser = meta_subparsers.add_parser('report', aliases = ['r'])
#    for meta_type_parser, sup_objs in [(etl_meta_parser, supported_etl.keys()), (rep_meta_parser, supported_reports.keys())]:
#        subparsers = meta_type_parser.add_subparsers(dest = 'meta_cmd', title = 'metadata subcommand')
#        append_cmd_subparsers(
#            cmd_subparsers = subparsers,
#            supported_objects = sup_objs,
#            append_all = True,
#            subcmd_name = 'print',
#            subcmd_aliases = ['p'],
#            subcmd_title = 'print metadata',
#            subcmd_dest = 'object_name',
#            subcmd_description = 'Object to print metadata',
#            process_all_func = print_all_meta,
#            process_obj_func = print_meta
#        )
#        append_cmd_subparsers(
#            cmd_subparsers = subparsers,
#            supported_objects = sup_objs,
#            append_all = True,
#            subcmd_name = 'drop',
#            subcmd_aliases = ['d'],
#            subcmd_title = 'drop metadata',
#            subcmd_dest = 'object_name',
#            subcmd_description = 'Object to drop metadata',
#            process_all_func = drop_all_meta,
#            process_obj_func = drop_meta
#        )

    if (len(sys.argv) > 1):
        args = main_parser.parse_args()
        args.exec_cmd_func(args)
    else:
        args = main_parser.parse_args('start_etl all'.split())
        args.exec_cmd_func(args)
        args = main_parser.parse_args('build_report all'.split())
        args.exec_cmd_func(args)

    print('INFO: finished')


main(
        supported_etl = get_supported_etl(),
        supported_reports = get_supported_reports()
)
