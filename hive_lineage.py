#!/usr/bin/python3
# coding=utf8

from sqllineage.runner import LineageRunner
import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter


def get_task_sql():
    file = open("sql_test/test.hql", "r")
    sql = file.read()
    file.close()
    df = sql.replace("SET hive.vectorized.execution.enabled=false;", "")
    return df

def list_lineages():
    df = get_task_sql()
    dataset_lineages = {}
    try:
        sql = df
        print("====================")
        result = LineageRunner(sql, dialect="hive")
        # 一个文件中有多个SQL语句，需要拆分处理
        if len(result.target_tables) > 2:
            print("目标表有多个，需要拆分SQL再计算血缘：【{}】".format(result.target_tables))
        else:
            dataset_lineages[str(result.target_tables[0])] = [str(t) for t in result.source_tables]

    except Exception as e:
        print("解析任务【{}】SQL失败。")
        print(e)

    return dataset_lineages

def generate_lineages():
        result_tables = list_lineages()
        for target_table in result_tables.keys():
            input_tables_urn = []
            for source_table in result_tables[target_table]:
                input_tables_urn.append(builder.make_dataset_urn("hive", source_table))

            # Construct a lineage object.
            lineage_mce = builder.make_lineage_mce(
                input_tables_urn,
                builder.make_dataset_urn("hive", target_table),
            )

            # Create an emitter to the GMS REST API.
            emitter = DatahubRestEmitter("http://192.168.5.25:8080")

            # Emit metadata!
            emitter.emit_mce(lineage_mce)
            try:
                emitter.emit_mce(lineage_mce)
                print("添加数仓表 【{}】血缘成功".format(target_table))
            except Exception as e:
                print("添加数仓表 【{}】血缘失败".format(target_table))
                print(e)
                break
    
if __name__ == "__main__":
    generate_lineages()