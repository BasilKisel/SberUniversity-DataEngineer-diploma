from enum import Enum
from datetime import datetime, timedelta
import csv
import zipfile
from zipfile import ZipFile
import openpyxl as xl

#####################################################################
### Abstract classes of the framework for DE1M                    ###
#####################################################################

class ResourceType(Enum):
    SQL_SCRIPT_DIR = 5 # pathlib.Path
    ARCHIVE_SHARE = 4 # pathlib.Path
    TARGET_DWH = 3 # connection
    FILE_SHARE = 2 # pathlib.Path
    BANK_OLTP = 1 # connection


class EtlPhase(Enum):
    LOAD_STAGE = 'LOAD_STAGE'
    CLEANUP = 'CLEANUP'
    LOAD_DWH = 'LOAD_DWH'
    BUILD_REPORT = 'BUILD_REPORT'


class AbstractSqlWrapper:
    def __init__(self):
        self.sqls = dict()

    def init_sql(self, *args):
        raise NotImplementedError()

    def _init_sql(self, path_to_sql_dir, *sql_file_subpaths):
        for subpath in sql_file_subpaths:
            with path_to_sql_dir.joinpath(*subpath) as sql_file_path:
                with sql_file_path.open(mode = 'rt', encoding = 'utf8') as file:
                    self.sqls[subpath] = '\n'.join(file.readlines())

    def _convert_timestamp(self, src_obj, trg_type):
        if (type(src_obj) == trg_type):
            return src_obj
        mappers = {
                (datetime, str): lambda x: datetime.strftime(src_obj, '%Y-%m-%d %H:%M:%S.%f'),
                (str, datetime): lambda x: datetime.strptime(src_obj, '%Y-%m-%d %H:%M:%S.%f')
        }
        conv_type = (type(src_obj), trg_type)
        if (conv_type in mappers):
            return mappers[conv_type](src_obj)
        else:
            raise ValueError('Not supported conversion: "{}" from "{}" to "{}".'.format(src_obj, type(src_obj), trg_type))


class AbstractWorker(AbstractSqlWrapper):
    def __init__(self):
        super().__init__()

    def get_db_resource_types(self, phase):
        raise NotImplementedError()

    def get_file_resource_types(self, phase):
        raise NotImplementedError()

    def perform(self, phase, src_res, meta):
        raise NotImplementedError()


class Metadata(AbstractSqlWrapper):
    SEL_BY_SCH_TBL = ('meta', 'meta_sel_by_sch_tbl.sql')
    MRG_INTEG_DT = ('meta', 'meta_mrg_integ_dt.sql')

    def __init__(self):
        super().__init__()

    def init_sql(self, path_to_sql_dir):
        self._init_sql(path_to_sql_dir, self.SEL_BY_SCH_TBL, self.MRG_INTEG_DT)

    def get_integrated_dt(self, dwh_curs, sch_name, tbl_name, trg_type):
        dwh_curs.execute(self.sqls[self.SEL_BY_SCH_TBL], [sch_name, tbl_name])
        integ_dt_str = dwh_curs.fetchone()[0]
        print('INFO: fetched last integrated dt for "{}"."{}" as "{}"'.format(sch_name, tbl_name, integ_dt_str))
        return self._convert_timestamp(integ_dt_str, trg_type)

    def set_integrated_dt(self, dwh_curs, sch_name, tbl_name, integ_dt):
        integ_dt = self._convert_timestamp(integ_dt, str)
        dwh_curs.execute(self.sqls[self.MRG_INTEG_DT], [sch_name, tbl_name, integ_dt])
        print('INFO: set last integrated date for "{}"."{}" as "{}"'.format(sch_name, tbl_name, integ_dt))


class Scd2LoadingStrategy:

    def load_dwh(
            self,
            dwh_curs,
            meta,
            sch_name,
            tbl_name,
            dwh_extr_del_rec,
            dwh_mrg_new_old_rec,
            dwh_add_upd_rec,
            stg_sel_last_upd_dt,
            new_upd_dt = None
            ):
        print('INFO: started DWH loading of {}.{}'.format(sch_name, tbl_name))
        dwh_curs.execute(dwh_extr_del_rec)
        dwh_curs.execute(dwh_mrg_new_old_rec)
        dwh_curs.execute(dwh_add_upd_rec)
        last_upd_dt_str = None
        if stg_sel_last_upd_dt:
            dwh_curs.execute(stg_sel_last_upd_dt)
            last_upd_dt_str = dwh_curs.fetchone()[0]
        meta.set_integrated_dt(dwh_curs, sch_name, tbl_name, last_upd_dt_str or new_upd_dt)
        print('INFO: finish DWH loading of {}.{}'.format(sch_name, tbl_name))
        return 0


class FactLoadingStrategy:

    def load_dwh(
            self,
            dwh_curs,
            meta,
            dwh_ins,
            sch_name,
            tbl_name,
            integ_dt
            ):
        print('INFO: started DWH loading of {}.{}'.format(sch_name, tbl_name))
        dwh_curs.execute(dwh_ins)
        meta.set_integrated_dt(dwh_curs, sch_name, tbl_name, integ_dt)
        print('INFO: finished DWH loading of {}.{}'.format(sch_name, tbl_name))
        return 0


class AbstractFileStaging:

    def _locate_file(self, path_to_dir, file_fs_fmt, file_dt_fmt, trg_dt = None):
        first_dt = None
        first_file = None
        for fs_item in path_to_dir.glob(file_fs_fmt):
            if fs_item.is_file():
                file_dt = datetime.strptime(fs_item.name, file_dt_fmt)
                if (trg_dt == file_dt):
                    return [file_dt, fs_item]
                elif not first_dt or first_dt > file_dt:
                    first_dt = file_dt
                    first_file = fs_item
        if trg_dt:
            return [None, None]
        else:
            return [first_dt, first_file]

    def get_file_to_load(self, dwh_curs, meta, path_to_dir, file_fs_fmt, file_dt_fmt, sch_name, tbl_name):
        last_dt = meta.get_integrated_dt(dwh_curs, sch_name, tbl_name, datetime)
        if last_dt < datetime(year = 2010, month = 1, day = 1):
            next_dt = None
        else:
            next_dt = last_dt + timedelta(days = 1)
        print('INFO: defined "{}" as target date for "{}"."{}".'.format(next_dt or 'first date', sch_name, tbl_name))

        file_dt, file_path = self._locate_file(path_to_dir, file_fs_fmt, file_dt_fmt, next_dt)
        if not file_path or not file_dt:
            print('WARNING: cannot locate source file for "{}"."{}" for date "{}".'.format(sch_name, tbl_name, next_dt or 'first date'))
        else:
            print('INFO: located file "{}" for date "{}".'.format(file_path, next_dt))
        return [file_dt, file_path]

    def move_to_archive(self, path_to_file, path_to_archive, file_dt_fmt, trg_dt):
        print('INFO: moving the file "{}" to the archive at "{}"'.format(path_to_file, path_to_archive.resolve()))
        arch_file_name = str(path_to_archive.joinpath(datetime.strftime(trg_dt, file_dt_fmt)))
        with ZipFile(arch_file_name, mode = 'a', compression = zipfile.ZIP_DEFLATED) as arch_file:
            arch_file.write(str(path_to_file))
        print('INFO: created the archive file "{}"'.format(arch_file_name))
        path_to_file.unlink()
        print('INFO: deleted the original file "{}"'.format(path_to_file))
        return 0
        
    def load_stage(
            self,
            dwh_curs,
            sch_name,
            tbl_name,
            row_iter,
            header,
            trg_fld_names,
            row_to_list,
            attr_validator,
            column_converter,
            stg_ins,
            stg_trc_lst,
            stg_ending_qry_lst = None
            ):
        print('INFO: started stage loading for "{}"."{}".'.format(sch_name, tbl_name))
        fetch_sz = dwh_curs.arraysize if dwh_curs.arraysize >= 1000 else 1000

        if not set(trg_fld_names) <= set(header):
            print('ERROR: file "{}" does not have proper header: columns {} expected, got {}.'.format(file_path, field_names, header))
            return 1

        for stg_trc in stg_trc_lst:
            dwh_curs.execute(stg_trc)
        print('INFO: truncated stage table.')

        row_cnt = 0
        batch_values = []
        for row in row_iter:
            attr = row_to_list(row)
            if not any(attr):
                continue
            attr = dict(zip(header, attr))
            if not attr_validator(attr):
                continue
            attr = column_converter(attr)
            
            values = []
            for fld_name in trg_fld_names:
                values.append(attr[fld_name])
            batch_values += [values]

            if (fetch_sz <= len(batch_values)):
                dwh_curs.executemany(stg_ins, batch_values)
                row_cnt += len(batch_values)
                print('INFO: loaded {} rows into the stage'.format(row_cnt))
                batch_values = []

        if len(batch_values) > 0:
            dwh_curs.executemany(stg_ins, batch_values)
            row_cnt += len(batch_values)

        print('INFO: finished loading: {} rows inserted into the stage'.format(row_cnt))
        if stg_ending_qry_lst:
            for qry in stg_ending_qry_lst:
                dwh_curs.execute(qry, [])

        print('INFO: finished stage loading for "{}"."{}".'.format(sch_name, tbl_name))

        return 0


class CsvStagingStrategy(AbstractFileStaging):

    def load_stage(
            self,
            dwh_curs,
            sch_name,
            tbl_name,
            file_path,
            field_names,
            csv_fmt_params,
            attr_validator,
            column_converter,
            stg_ins,
            stg_trc_lst,
            stg_ending_qry_lst = None
            ):
        with file_path.open(mode = 'rt', newline = '') as file_obj:
            csv_reader = csv.reader(file_obj, **csv_fmt_params)
            csv_row_iter = iter(csv_reader)
            header = next(csv_row_iter)

            return super().load_stage(
                    dwh_curs = dwh_curs,
                    sch_name = sch_name,
                    tbl_name = tbl_name,
                    row_iter = csv_row_iter,
                    header = header,
                    trg_fld_names = field_names,
                    row_to_list = (lambda row: row),
                    attr_validator = attr_validator,
                    column_converter = column_converter,
                    stg_ins = stg_ins,
                    stg_trc_lst = stg_trc_lst,
                    stg_ending_qry_lst = stg_ending_qry_lst,
                    )

class XlsxStagingStrategy(AbstractFileStaging):

    def load_stage(
            self,
            dwh_curs,
            sch_name,
            tbl_name,
            file_path,
            sheet_name,
            field_names,
            attr_validator,
            column_converter,
            stg_ins,
            stg_trc_lst,
            stg_ending_qry_lst = None
            ):
        wb = None
        try:
            wb = xl.load_workbook(filename = str(file_path), read_only = True)
            ws = wb[sheet_name]
            row_iter = iter(ws.rows)
            row_to_list = (lambda row: list(map(lambda c: c.value, iter(row))))
            header = row_to_list(next(row_iter))

            return super().load_stage(
                dwh_curs = dwh_curs,
                row_iter = row_iter,
                header = header,
                trg_fld_names = field_names,
                row_to_list = row_to_list,
                attr_validator = attr_validator,
                column_converter = column_converter,
                stg_ins = stg_ins,
                stg_trc_lst = stg_trc_lst,
                sch_name = sch_name,
                tbl_name = tbl_name,
                stg_ending_qry_lst = stg_ending_qry_lst
            )
        finally:
            if wb:
                wb.close()
        

#####################################################################
### Database to database related stagers and loaders              ###
#####################################################################

class BankStagingStrategy():

    def load_stage(
            self,
            dwh_curs,
            bank_curs,
            meta,
            src_sel_by_dt,
            stg_ins,
            stg_trc,
            src_sel_act_rec,
            stg_act_rec_trc,
            stg_act_rec_ins,
            sch_name,
            tbl_name
            ):
        print('INFO: started stage loading for "{}"."{}"'.format(sch_name, tbl_name))
        fetch_sz = dwh_curs.arraysize if dwh_curs.arraysize >= 1000 else 1000

        dwh_curs.execute(stg_trc)
        print('INFO: truncated the incremental stage')

        dwh_curs.execute(stg_act_rec_trc)
        print('INFO: truncated the stage of active records')

        last_integ_dt = meta.get_integrated_dt(dwh_curs, sch_name, tbl_name, str)

        rec_cnt = 0
        bank_curs.execute(src_sel_by_dt, ([last_integ_dt] * 2))
        entity_attr = bank_curs.fetchmany(fetch_sz)
        while entity_attr:
            # порядок полей должен совпадать в sql запросах
            dwh_curs.executemany(stg_ins, entity_attr)
            rec_cnt += len(entity_attr)
            print('INFO: loaded {} rows to the incremental stage'.format(rec_cnt))
            entity_attr = bank_curs.fetchmany(fetch_sz)
        print('INFO: finished loading: {} rows loaded to the incremental stage'.format(rec_cnt))

        bank_curs.execute(src_sel_act_rec)
        entity_pk = bank_curs.fetchmany(fetch_sz)
        while entity_pk:
            dwh_curs.executemany(stg_act_rec_ins, entity_pk)
            rec_cnt += len(entity_pk)
            entity_pk = bank_curs.fetchmany(fetch_sz)
        print('INFO: loaded {} rows to the stage of active records'.format(rec_cnt))
        print('INFO: finished stage loading for "{}"."{}"'.format(sch_name, tbl_name))
        return 0


class BankLoader(AbstractWorker):

    def __init__(
            self,
            sch_name,
            tbl_name,
            src_sel_by_dt,
            stg_ins,
            stg_trc,
            src_sel_act_rec,
            stg_act_rec_trc,
            stg_act_rec_ins,
            dwh_extr_del_rec,
            dwh_mrg_new_old_rec,
            dwh_add_upd_rec,
            stg_sel_last_upd_dt,
            ):
        super().__init__()
        self.SCH_NAME = sch_name
        self.TBL_NAME = tbl_name
        self.SRC_SEL_BY_DT = src_sel_by_dt
        self.STG_INS = stg_ins
        self.STG_TRC = stg_trc
        self.SRC_SEL_ACT_REC = src_sel_act_rec
        self.STG_ACT_REC_TRC = stg_act_rec_trc
        self.STG_ACT_REC_INS = stg_act_rec_ins
        self.DWH_EXTR_DEL_REC = dwh_extr_del_rec
        self.DWH_MRG_NEW_OLD_REC = dwh_mrg_new_old_rec
        self.DWH_ADD_UPD_REC = dwh_add_upd_rec
        self.STG_SEL_LAST_UPD_DT = stg_sel_last_upd_dt

        self.staging_strategy = BankStagingStrategy()
        self.dwh_strategy = Scd2LoadingStrategy()

    def init_sql(self, path_to_sql_dir):
        self._init_sql(
                path_to_sql_dir,
                self.SRC_SEL_BY_DT,
                self.STG_INS,
                self.STG_TRC,
                self.SRC_SEL_ACT_REC,
                self.STG_ACT_REC_TRC,
                self.STG_ACT_REC_INS,
                self.DWH_ADD_UPD_REC,
                self.DWH_EXTR_DEL_REC,
                self.DWH_MRG_NEW_OLD_REC,
                self.STG_SEL_LAST_UPD_DT
        )

    def get_db_resource_types(self, phase):
        if (phase == EtlPhase.LOAD_STAGE):
            return [ResourceType.TARGET_DWH, ResourceType.BANK_OLTP]
        elif (EtlPhase.LOAD_DWH == phase):
            return [ResourceType.TARGET_DWH]
        elif (phase == EtlPhase.CLEANUP):
            return []
        else:
            raise NotImplementedError()

    def get_file_resource_types(self, phase):
        if (phase == EtlPhase.LOAD_STAGE):
            return []
        elif (EtlPhase.LOAD_DWH == phase):
            return []
        elif (phase == EtlPhase.CLEANUP):
            return []
        else:
            raise NotImplementedError()

    def perform(self, phase, resources, meta):
        if (phase == EtlPhase.LOAD_STAGE):
            return self.staging_strategy.load_stage(
                dwh_curs = resources[ResourceType.TARGET_DWH],
                bank_curs = resources[ResourceType.BANK_OLTP],
                meta = meta,
                src_sel_by_dt = self.sqls[self.SRC_SEL_BY_DT],
                stg_ins = self.sqls[self.STG_INS],
                stg_trc = self.sqls[self.STG_TRC],
                src_sel_act_rec = self.sqls[self.SRC_SEL_ACT_REC],
                stg_act_rec_trc = self.sqls[self.STG_ACT_REC_TRC],
                stg_act_rec_ins = self.sqls[self.STG_ACT_REC_INS],
                sch_name = self.SCH_NAME,
                tbl_name = self.TBL_NAME
            )
        elif (EtlPhase.LOAD_DWH == phase):
            return self.dwh_strategy.load_dwh(
                dwh_curs = resources[ResourceType.TARGET_DWH],
                meta = meta,
                dwh_extr_del_rec = self.sqls[self.DWH_EXTR_DEL_REC],
                dwh_mrg_new_old_rec = self.sqls[self.DWH_MRG_NEW_OLD_REC],
                dwh_add_upd_rec = self.sqls[self.DWH_ADD_UPD_REC],
                stg_sel_last_upd_dt = self.sqls[self.STG_SEL_LAST_UPD_DT],
                sch_name = self.SCH_NAME,
                tbl_name = self.TBL_NAME
            )
        elif (phase == EtlPhase.CLEANUP):
            return 0
        else:
            raise NotImplementedError()


class AccountLoader(BankLoader):

    def __init__(self):
        super().__init__(
            sch_name = 'DE1M',
            tbl_name = 'KISL_DWH_DIM_ACCOUNTS_HIST',
            src_sel_by_dt = ('accounts', 'src_accounts_sel_by_dt.sql'),
            stg_ins = ('accounts', 'stg_accounts_ins.sql'),
            stg_trc = ('accounts', 'stg_accounts_trc.sql'),
            src_sel_act_rec = ('accounts', 'src_accounts_sel_act_rec.sql'),
            stg_act_rec_trc = ('accounts', 'stg_accounts_act_rec_trc.sql'),
            stg_act_rec_ins = ('accounts', 'stg_accounts_act_rec_ins.sql'),
            dwh_extr_del_rec = ('accounts', 'dwh_accounts_extr_del_rec.sql'),
            dwh_mrg_new_old_rec = ('accounts', 'dwh_accounts_mrg_new_old_rec.sql'),
            dwh_add_upd_rec = ('accounts', 'dwh_accounts_add_upd_rec.sql'),
            stg_sel_last_upd_dt = ('accounts', 'stg_accounts_sel_last_upd_dt.sql')
        )


class CardLoader(BankLoader):

    def __init__(self):
        super().__init__(
            sch_name = 'DE1M',
            tbl_name = 'KISL_DWH_DIM_CARDS_HIST',
            src_sel_by_dt = ('cards', 'src_cards_sel_by_dt.sql'),
            stg_ins = ('cards', 'stg_cards_ins.sql'),
            stg_trc = ('cards', 'stg_cards_trc.sql'),
            src_sel_act_rec = ('cards', 'src_cards_sel_act_rec.sql'),
            stg_act_rec_trc = ('cards', 'stg_cards_act_rec_trc.sql'),
            stg_act_rec_ins = ('cards', 'stg_cards_act_rec_ins.sql'),
            dwh_extr_del_rec = ('cards', 'dwh_cards_extr_del_rec.sql'),
            dwh_mrg_new_old_rec = ('cards', 'dwh_cards_mrg_new_old_rec.sql'),
            dwh_add_upd_rec = ('cards', 'dwh_cards_add_upd_rec.sql'),
            stg_sel_last_upd_dt = ('cards', 'stg_cards_sel_last_upd_dt.sql')
        )


class ClientLoader(BankLoader):

    def __init__(self):
        super().__init__(
            sch_name = 'DE1M',
            tbl_name = 'KISL_DWH_DIM_CLIENTS_HIST',
            src_sel_by_dt = ('clients', 'src_clients_sel_by_dt.sql'),
            stg_ins = ('clients', 'stg_clients_ins.sql'),
            stg_trc = ('clients', 'stg_clients_trc.sql'),
            src_sel_act_rec = ('clients', 'src_clients_sel_act_rec.sql'),
            stg_act_rec_trc = ('clients', 'stg_clients_act_rec_trc.sql'),
            stg_act_rec_ins = ('clients', 'stg_clients_act_rec_ins.sql'),
            dwh_extr_del_rec = ('clients', 'dwh_clients_extr_del_rec.sql'),
            dwh_mrg_new_old_rec = ('clients', 'dwh_clients_mrg_new_old_rec.sql'),
            dwh_add_upd_rec = ('clients', 'dwh_clients_add_upd_rec.sql'),
            stg_sel_last_upd_dt = ('clients', 'stg_clients_sel_last_upd_dt.sql')
        )


#####################################################################
### Files related stagers and loaders                             ###
#####################################################################

class FileLoader(AbstractWorker):

    def __init__(self):
        super().__init__()

    def init_sql(self, path_to_sql_dir, *sql_file_tuples):
        self._init_sql(
                path_to_sql_dir,
                *sql_file_tuples
        )

    def get_db_resource_types(self, phase):
        if (phase == EtlPhase.LOAD_STAGE):
            return [ResourceType.TARGET_DWH]
        elif (EtlPhase.LOAD_DWH == phase):
            return [ResourceType.TARGET_DWH]
        elif (phase == EtlPhase.CLEANUP):
            return []
        else:
            raise NotImplementedError()

    def get_file_resource_types(self, phase):
        if (phase == EtlPhase.LOAD_STAGE):
            return [ResourceType.FILE_SHARE]
        elif (EtlPhase.LOAD_DWH == phase):
            return []
        elif (phase == EtlPhase.CLEANUP):
            return [ResourceType.FILE_SHARE, ResourceType.ARCHIVE_SHARE]
        else:
            raise NotImplementedError()

    def perform(self, phase, resources, meta):
        if (phase == EtlPhase.LOAD_STAGE):
            return self._load_stage(resources, meta)
        elif (EtlPhase.LOAD_DWH == phase):
            return self._load_dwh(resources, meta)
        elif (phase == EtlPhase.CLEANUP):
            return self._cleanup(resources, meta)
        else:
            raise NotImplementedError()

    def _load_stage(self, resources, meta):
        raise NotImplementedError()

    def _load_dwh(self, resources, meta):
        raise NotImplementedError()

    def _cleanup(self, resources, meta):
        raise NotImplementedError()


class TransactionLoader(FileLoader):
    STG_INS = ('transactions', 'stg_transactions_ins.sql')
    STG_TRC = ('transactions', 'stg_transactions_trc.sql')
    DWH_INS = ('transactions', 'dwh_transactions_ins.sql')

    def __init__(self):
        super().__init__()
        self.staging_strategy = CsvStagingStrategy()
        self.dwh_strategy = FactLoadingStrategy()
        self.file_dt = None
        self.file_path = None

    def init_sql(self, path_to_sql_dir):
        self._init_sql(
                path_to_sql_dir,
                self.STG_INS,
                self.STG_TRC,
                self.DWH_INS
        )

    def _load_stage(self, resources, meta):
        self.file_dt, self.file_path = self.staging_strategy.get_file_to_load(
                dwh_curs = resources[ResourceType.TARGET_DWH],
                meta = meta,
                path_to_dir = resources[ResourceType.FILE_SHARE],
                file_fs_fmt = 'transactions_*.txt',
                file_dt_fmt = 'transactions_%d%m%Y.txt',
                sch_name = 'DE1M',
                tbl_name = 'KISL_DWH_FACT_TRANSACTIONS'
            )
        if not self.file_path:
            print('WARNING: cannot start stage loading: no file to load')
            return 1
        return self.staging_strategy.load_stage(
                dwh_curs = resources[ResourceType.TARGET_DWH],
                file_path = self.file_path,
                field_names = ['transaction_id', 'transaction_date', 'amount', 'card_num', 'oper_type', 'oper_result', 'terminal'],
                csv_fmt_params = {'delimiter': ';', 'quoting': csv.QUOTE_NONE, 'strict': True},
                attr_validator = lambda row: True,
                column_converter = lambda d: d,
                stg_ins = self.sqls[self.STG_INS],
                stg_trc_lst = [self.sqls[self.STG_TRC]],
                sch_name = 'DE1M',
                tbl_name = 'KISL_DWH_FACT_TRANSACTIONS'
            )

    def _load_dwh(self, resources, meta):
        if not self.file_dt:
            print('INFO: cannot start DWH loading before stage loading: no stage date for file "{}"'.format(file_path))
            return 1
        return self.dwh_strategy.load_dwh(
                dwh_curs = resources[ResourceType.TARGET_DWH],
                meta = meta,
                dwh_ins = self.sqls[self.DWH_INS],
                sch_name = 'DE1M',
                tbl_name = 'KISL_DWH_FACT_TRANSACTIONS',
                integ_dt = self.file_dt
            )

    def _cleanup(self, resources, meta):
        if not self.file_dt:
            print('INFO: cannot start cleanup: no stage date for file "{}"'.format(file_path))
            return 1
        return self.staging_strategy.move_to_archive(
                path_to_file = self.file_path,
                path_to_archive = resources[ResourceType.ARCHIVE_SHARE],
                file_dt_fmt = 'transactions_y%Ym%md%d.zip',
                trg_dt = self.file_dt
            )


class TerminalLoader(FileLoader):
    STG_INS = ('terminals', 'stg_terminals_ins.sql')
    STG_TRC = ('terminals', 'stg_terminals_trc.sql')
    STG_ACT_REC_INS = ('terminals', 'stg_terminals_act_rec_ins.sql')
    STG_ACT_REC_TRC = ('terminals', 'stg_terminals_act_rec_trc.sql')
    DWH_EXTR_DEL_REC = ('terminals', 'dwh_terminals_extr_del_rec.sql')
    DWH_MRG_NEW_OLD_REC = ('terminals', 'dwh_terminals_mrg_new_old_rec.sql')
    DWH_ADD_UPD_REC = ('terminals', 'dwh_terminals_add_upd_rec.sql')

    def __init__(self):
        super().__init__()
        self.sch_name = 'DE1M'
        self.tbl_name = 'KISL_DWH_DIM_TERMINALS_HIST'
        self.staging_strategy = XlsxStagingStrategy()
        self.dwh_strategy = Scd2LoadingStrategy()
        self.file_dt = None
        self.file_path = None

    def init_sql(self, path_to_sql_dir):
        self._init_sql(
                path_to_sql_dir,
                self.STG_INS,
                self.STG_TRC,
                self.STG_ACT_REC_INS,
                self.STG_ACT_REC_TRC,
                self.DWH_EXTR_DEL_REC,
                self.DWH_MRG_NEW_OLD_REC,
                self.DWH_ADD_UPD_REC,
        )

    def _load_stage(self, resources, meta):
        self.file_dt, self.file_path = self.staging_strategy.get_file_to_load(
                dwh_curs = resources[ResourceType.TARGET_DWH],
                meta = meta,
                path_to_dir = resources[ResourceType.FILE_SHARE],
                file_fs_fmt = 'terminals_*.xlsx',
                file_dt_fmt = 'terminals_%d%m%Y.xlsx',
                sch_name = self.sch_name,
                tbl_name = self.tbl_name
            )
        if not self.file_path:
            print('INFO: cannot start stage loading: no file to load')
            return 1
        return self.staging_strategy.load_stage(
                dwh_curs = resources[ResourceType.TARGET_DWH],
                file_path = self.file_path,
                sheet_name = 'terminals',
                field_names = ['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address'],
                attr_validator = lambda row: True,
                column_converter = lambda d: d,
                stg_ins = self.sqls[self.STG_INS],
                stg_trc_lst = [self.sqls[self.STG_TRC], self.sqls[self.STG_ACT_REC_TRC]],
                sch_name = self.sch_name,
                tbl_name = self.tbl_name,
                stg_ending_qry_lst = [self.sqls[self.STG_ACT_REC_INS]]
            )

    def _load_dwh(self, resources, meta):
        if not self.file_dt:
            print('INFO: cannot start DWH loading before stage loading: no stage date for file "{}"'.format(file_path))
            return 1
        return self.dwh_strategy.load_dwh(
                dwh_curs = resources[ResourceType.TARGET_DWH],
                meta = meta,
                dwh_extr_del_rec = self.sqls[self.DWH_EXTR_DEL_REC],
                dwh_mrg_new_old_rec = self.sqls[self.DWH_MRG_NEW_OLD_REC],
                dwh_add_upd_rec = self.sqls[self.DWH_ADD_UPD_REC],
                stg_sel_last_upd_dt = None,
                new_upd_dt = self.file_dt,
                sch_name = self.sch_name,
                tbl_name = self.tbl_name,
            )

    def _cleanup(self, resources, meta):
        if not self.file_dt:
            print('INFO: cannot start cleanup: no stage date for file "{}"'.format(file_path))
            return 1
        return self.staging_strategy.move_to_archive(
                path_to_file = self.file_path,
                path_to_archive = resources[ResourceType.ARCHIVE_SHARE],
                file_dt_fmt = 'terminals_y%Ym%md%d.zip',
                trg_dt = self.file_dt
            )


class PassportBlacklistLoader(FileLoader):
    STG_INS = ('blacklist', 'stg_blacklist_ins.sql')
    STG_TRC = ('blacklist', 'stg_blacklist_trc.sql')
    DWH_INS = ('blacklist', 'dwh_blacklist_ins.sql')

    def __init__(self):
        super().__init__()
        self.sch_name = 'DE1M'
        self.tbl_name = 'KISL_DWH_FACT_PSSPRT_BLCKLST'
        self.staging_strategy = XlsxStagingStrategy()
        self.dwh_strategy = FactLoadingStrategy()
        self.file_dt = None
        self.file_path = None

    def init_sql(self, path_to_sql_dir):
        self._init_sql(
                path_to_sql_dir,
                self.STG_INS,
                self.STG_TRC,
                self.DWH_INS
            )

    def _load_stage(self, resources, meta):
        def replace_dt(d):
            d['date'] = datetime.strftime(d['date'], '%Y-%m-%d %H-%M-%S')
            return d

        self.file_dt, self.file_path = self.staging_strategy.get_file_to_load(
                dwh_curs = resources[ResourceType.TARGET_DWH],
                meta = meta,
                path_to_dir = resources[ResourceType.FILE_SHARE],
                file_fs_fmt = 'passport_blacklist_*.xlsx',
                file_dt_fmt = 'passport_blacklist_%d%m%Y.xlsx',
                sch_name = self.sch_name,
                tbl_name = self.tbl_name
            )
        if not self.file_path:
            print('INFO: cannot start stage loading: no file to load')
            return 1
        return self.staging_strategy.load_stage(
                dwh_curs = resources[ResourceType.TARGET_DWH],
                file_path = self.file_path,
                sheet_name = 'blacklist',
                field_names = ['passport', 'date'],
                attr_validator = (lambda d: d['date'] >= self.file_dt) ,
                column_converter = replace_dt,
                stg_ins = self.sqls[self.STG_INS],
                stg_trc_lst = [self.sqls[self.STG_TRC]],
                sch_name = self.sch_name,
                tbl_name = self.tbl_name,
                stg_ending_qry_lst = None
            )

    def _load_dwh(self, resources, meta):
        if not self.file_dt:
            print('INFO: cannot start DWH loading before stage loading: no stage date for file "{}"'.format(file_path))
            return 1
        return self.dwh_strategy.load_dwh(
                dwh_curs = resources[ResourceType.TARGET_DWH],
                meta = meta,
                dwh_ins = self.sqls[self.DWH_INS],
                sch_name = self.sch_name,
                tbl_name = self.tbl_name,
                integ_dt = self.file_dt
            )

    def _cleanup(self, resources, meta):
        if not self.file_dt:
            print('INFO: cannot start cleanup: no stage date for file "{}"'.format(file_path))
            return 1
        return self.staging_strategy.move_to_archive(
                path_to_file = self.file_path,
                path_to_archive = resources[ResourceType.ARCHIVE_SHARE],
                file_dt_fmt = 'passport_blacklist_y%Ym%md%d.zip',
                trg_dt = self.file_dt
            )


#####################################################################
### Report builders                                               ###
#####################################################################

class FraudReportBuilder(AbstractSqlWrapper):
    FRAUD_FIRST_TYPE = ('rep_fraud', 'fraud_first_type.sql')
    FRAUD_SECOND_TYPE = ('rep_fraud', 'fraud_second_type.sql')
    FRAUD_THIRD_TYPE = ('rep_fraud', 'fraud_third_type.sql')
    FRAUD_FORTH_TYPE = ('rep_fraud', 'fraud_forth_type.sql')

    def __init__(self):
        super().__init__()

    def init_sql(self, sql_dir_path):
        self._init_sql(
                sql_dir_path, 
                self.FRAUD_FIRST_TYPE, 
                self.FRAUD_FORTH_TYPE, 
                self.FRAUD_SECOND_TYPE, 
                self.FRAUD_THIRD_TYPE
                )

    def get_db_resource_types(self, phase):
        if (phase == EtlPhase.BUILD_REPORT):
            return [ResourceType.TARGET_DWH]
        raise NotImplementedError()

    def get_file_resource_types(self, phase):
        return []

    def perform(self, phase, src_res, meta):
        if (phase == EtlPhase.BUILD_REPORT):
            today_db_dt_str = datetime.today().strftime('%Y-%m-%d %H-%M-%S')
            dwh_curs = src_res[ResourceType.TARGET_DWH]
            print('INFO: started build the fraud report')
            dwh_curs.execute(self.sqls[self.FRAUD_FIRST_TYPE], [today_db_dt_str])
            dwh_curs.execute(self.sqls[self.FRAUD_SECOND_TYPE], [today_db_dt_str])
            dwh_curs.execute(self.sqls[self.FRAUD_THIRD_TYPE], [today_db_dt_str])
            dwh_curs.execute(self.sqls[self.FRAUD_FORTH_TYPE], [today_db_dt_str])
            print('INFO: finished build the fraud report')
            return 0
        raise NotImplementedError()

#####################################################################
### Global resources                                              ###
#####################################################################

def get_supported_etl():
    acc_loader = AccountLoader()
    card_loader = CardLoader()
    client_loader = ClientLoader()
    tran_loader = TransactionLoader()
    term_loader = TerminalLoader()
    blacklist_loader = PassportBlacklistLoader()
    return {
            'accounts': acc_loader,
            'cards': card_loader,
            'clients': client_loader,
            'transactions': tran_loader,
            'terminals': term_loader,
            'blacklist': blacklist_loader,
    }

def get_supported_reports():
    fraud_rep = FraudReportBuilder()
    return {
            'fraud': fraud_rep
    }
