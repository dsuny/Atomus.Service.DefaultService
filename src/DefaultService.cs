using Atomus.Attribute;
using Atomus.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data;
using System.Transactions;

namespace Atomus.Service
{
    /// <summary>
    /// 기본 서비스를 구현합니다.
    /// </summary>
    #region
    [Author("권대선", 2017, 9, 25, AuthorAttributeType.Create, @"
    /// <summary>
    /// 기본 서비스를 구현합니다.
    /// </summary>")]
    #endregion
    public class DefaultService : IService
    {
        private readonly int serviceTimeout;
        private readonly Database.IDatabaseAdapter databaseAdapter;

        /// <summary>
        /// 생성자 입니다.
        /// 서비스 타임 시간(ms)을 가져옵니다.
        /// databaseAdapter를 생성합니다.
        /// </summary>
        #region
        [Author("권대선", 2017, 9, 25, AuthorAttributeType.Create, @"
        /// <summary>
        /// 생성자 입니다.
        /// 서비스 타임 시간(ms)을 가져옵니다.
        /// databaseAdapter를 생성합니다.
        /// </summary>")]
        [HistoryComment("권대선", 2017, 10, 9, AuthorAttributeType.Modify, @"
        /// <summary>
        /// databaseAdapter 생성 추가
        /// </summary>")]
        #endregion
        public DefaultService()
        {
            try
            {
                this.serviceTimeout = this.GetAttribute("ServiceTimeout").ToInt();
            }
            catch (Exception exception)
            {
                DiagnosticsTool.MyTrace(exception);
                this.serviceTimeout = 60000;
            }

            try
            {
                databaseAdapter = (Database.IDatabaseAdapter)Factory.CreateInstance("Atomus.Database.DatabaseAdapter");
                //databaseAdapter = new Database.DatabaseAdapter();

                //databaseAdapter = (Database.IDatabaseAdapter)Factory.CreateInstance(@"E:\Work\Project\Atomus\Database\DatabaseAdapter\bin\Debug\Atomus.Database.DatabaseAdapter.dll", "Atomus.Database.DatabaseAdapter", true, true);
            }
            catch (Exception exception)
            {
                DiagnosticsTool.MyTrace(exception);
            }
        }

        #region
        [HistoryComment("권대선", 2017, 10, 9, AuthorAttributeType.Modify, @"
        /// <summary>
        /// IServiceDataSet.TransactionScope 이 true로 설정 되어 있으면 TransactionScope 블록으로 실행합니다.
        /// IServiceDataSet.TransactionScope 이 false로 설정 되어 있으면 바로 실행합니다.
        ///
        /// databaseAdapter를 멤버로 이동하여 중복적인 인스턴스 생성를 방지
        ///
        /// ServiceDataSet 속성 값 중에 ""GetDatabaseNames""이 있으면 DatabaseNames을 가져옵니다.
        /// </summary>")]
        #endregion
        Response IService.Request(ServiceDataSet serviceDataSet)
        {
            IResponse response;

            try
            {
                if (!serviceDataSet.ServiceName.Equals("Atomus.Service.DefaultService"))
                    throw new Exception("Not Atomus.Service.DefaultService");

                if (((IServiceDataSet)serviceDataSet).GetAttribute("GetDatabaseConnectionNames") != null)
                    return GetDatabaseConnectionNames();

                ((IServiceDataSet)serviceDataSet).CreateServiceDataTable();

                if (((IServiceDataSet)serviceDataSet).TransactionScope)
                    using (TransactionScope transactionScope = new TransactionScope(TransactionScopeOption.Required, new TimeSpan(0, 0, 0, 0, serviceTimeout)))
                    {
                        response = this.Excute(serviceDataSet);

                        if (response.Status == Status.OK)
                            transactionScope.Complete();
                    }
                else
                    response = this.Excute(serviceDataSet);

            }
            catch (AtomusException exception)
            {
                DiagnosticsTool.MyTrace(exception);
                return (Response)Factory.CreateInstance("Atomus.Service.Response", false, true, exception);
            }
            catch (Exception exception)
            {
                DiagnosticsTool.MyTrace(exception);
                return (Response)Factory.CreateInstance("Atomus.Service.Response", false, true, exception);
            }

            return (Response)response;
        }

        #region
        [Author("권대선", 2017, 9, 25, AuthorAttributeType.Create, @"
        /// <summary>
        /// 기본 서비스를 수행 합니다.
        /// </summary>
        /// <returns>서비스 처리 결과를 반환합니다.</returns>")]
        [HistoryComment("권대선", 2017, 10, 23, AuthorAttributeType.Modify, @"
        /// <summary>
        /// Command.CommandType을 serviceDataSet에서 가져 오도록 수정함
        /// database.Command.CommandType != CommandType.Text 인 경우에만 파라미터 처리하도록 수정
        /// database.Command.CommandType == CommandType.Text 인 경우에 database.Command.CommandText = (string)dataRow[""Query""] 로 수정
        /// </summary>")]
        #endregion
        Response Excute(ServiceDataSet serviceDataSet)
        {
            Dictionary<string, Database.IDatabase> databaseList;
            Database.IDatabase database;
            IResponse response;
            DataSet dataSet;
            DataTable dataTable;
            int tableCount;
            DataTable outPutTable;
            DataRow outPutDataRow;
            DataTable outPutTable1;

            response = (IResponse)Factory.CreateInstance("Atomus.Service.Response", false, true);
            databaseList = null;

            try
            {
                databaseList = new Dictionary<string, Database.IDatabase>();

                this.CreateAndOpenDatabase(databaseList, serviceDataSet);

                outPutTable = new DataTable("OutPutTable");
                outPutTable.Columns.Add("SourceTableName", Type.GetType("System.String"));
                outPutTable.Columns.Add("SourceParameterName", Type.GetType("System.String"));
                outPutTable.Columns.Add("TargetTableName", Type.GetType("System.String"));
                outPutTable.Columns.Add("TargetParameterName", Type.GetType("System.String"));
                outPutTable.Columns.Add("Value", Type.GetType("System.Object"));

                dataSet = new DataSet();//결과 저장 DataSet
                tableCount = 0;
                foreach (DataTable table in ((IServiceDataSet)serviceDataSet).DataTables)
                {
                    database = databaseList[((IServiceDataSet)serviceDataSet)[table.TableName].ConnectionName];

                    //파라미터 생성
                    if (database.Command.CommandType != System.Data.CommandType.Text)
                        foreach (DataColumn dataColumn in table.Columns)
                        {
                            System.Data.Common.DbParameter dbParameter;
                            Database.DbType dbType;

                            dbType = (Database.DbType)Enum.ToObject(typeof(Database.DbType), ((IServiceDataSet)serviceDataSet)[table.TableName].GetAttribute(dataColumn.ColumnName, "DbType").ToString().ToInt());

                            dbParameter = database.AddParameter(dataColumn.ColumnName, dbType, ((IServiceDataSet)serviceDataSet)[table.TableName].GetAttribute(dataColumn.ColumnName, "Size").ToString().ToInt());

                            ///Target이 있는 파라미터이면
                            if (((IServiceDataSet)serviceDataSet)[table.TableName].GetAttribute(dataColumn.ColumnName, "TargetTableName") != null)
                            {
                                dbParameter.Direction = ParameterDirection.InputOutput;
                                outPutDataRow = outPutTable.NewRow();
                                outPutDataRow["SourceTableName"] = table.TableName;
                                outPutDataRow["SourceParameterName"] = dataColumn.ColumnName;
                                outPutDataRow["TargetTableName"] = ((IServiceDataSet)serviceDataSet)[table.TableName].GetAttribute(dataColumn.ColumnName, "TargetTableName");
                                outPutDataRow["TargetParameterName"] = ((IServiceDataSet)serviceDataSet)[table.TableName].GetAttribute(dataColumn.ColumnName, "TargetParameterName");
                                outPutTable.Rows.Add(outPutDataRow);
                            }
                            else
                                dbParameter.Direction = ParameterDirection.Input;
                        }

                    foreach (DataRow dataRow in table.Rows)
                    {
                        //파라미터 값 입력
                        if (database.Command.CommandType != System.Data.CommandType.Text)
                            foreach (DataColumn dataColumn in table.Columns)
                                //_OutPutTable에 있는 항목인지
                                if (outPutTable.Select($"TargetTableName='{table.TableName}' AND TargetParameterName='{dataColumn.ColumnName}'").Length > 0)
                                    database.Command.Parameters[dataColumn.ColumnName].Value = outPutTable.Select($"TargetTableName='{table.TableName}' AND TargetParameterName='{dataColumn.ColumnName}'")[0]["Value"];
                                else
                                    database.Command.Parameters[dataColumn.ColumnName].Value = dataRow[dataColumn.ColumnName];

                        //프로시져명
                        switch (database.Command.CommandType)
                        {
                            case System.Data.CommandType.Text:
                                database.Command.CommandText = (string)dataRow["Query"];
                                break;
                            case System.Data.CommandType.StoredProcedure:
                                database.Command.CommandText = (string)((IServiceDataSet)serviceDataSet)[table.TableName].CommandText;
                                break;
                            case System.Data.CommandType.TableDirect:
                                break;
                        }

                        database.DataAdapter.Fill(dataSet);

                        while (dataSet.Tables.Count != 0)
                        {
                            dataTable = dataSet.Tables[0];
                            dataTable.TableName = tableCount.ToString();
                            dataSet.Tables.Remove(dataTable);
                            response.DataSet.Tables.Add(dataTable);

                            tableCount += 1;
                        }

                        foreach (System.Data.Common.DbParameter dbParameter in database.Command.Parameters)
                            if (dbParameter.Direction == ParameterDirection.InputOutput)
                                if (outPutTable.Select($"SourceTableName='{table.TableName}' AND SourceParameterName='{dbParameter.ParameterName}'").Length > 0)
                                    outPutTable.Select($"SourceTableName='{table.TableName}' AND SourceParameterName='{dbParameter.ParameterName}'")[0]["Value"] = dbParameter.Value;
                    }

                    database.Command.Parameters.Clear();
                }

                if (response.DataSet.Tables.Count < 1)
                {
                    if (outPutTable.Rows.Count > 0)
                    {
                        outPutTable1 = new DataTable("OutPutTable");
                        outPutTable1.Columns.Add("SourceTableName", Type.GetType("System.String"));
                        outPutTable1.Columns.Add("SourceParameterName", Type.GetType("System.String"));
                        outPutTable1.Columns.Add("TargetTableName", Type.GetType("System.String"));
                        outPutTable1.Columns.Add("TargetParameterName", Type.GetType("System.String"));
                        outPutTable1.Columns.Add("Value", Type.GetType("System.String"));

                        foreach (DataRow dataRow in outPutTable.Rows)
                        {
                            outPutDataRow = outPutTable1.NewRow();
                            outPutTable1.Rows.Add(outPutDataRow);
                            outPutDataRow["SourceTableName"] = dataRow["SourceTableName"];
                            outPutDataRow["SourceParameterName"] = dataRow["SourceParameterName"];
                            outPutDataRow["TargetTableName"] = dataRow["TargetTableName"];
                            outPutDataRow["TargetParameterName"] = dataRow["TargetParameterName"];
                            outPutDataRow["Value"] = dataRow["Value"].ToString();
                        }

                        response.DataSet.Tables.Add(outPutTable1);
                    }
                    else response.DataSet = null;
                }
                else
                {
                    if (outPutTable.Rows.Count > 0)
                    {
                        outPutTable1 = new DataTable("OutPutTable");
                        outPutTable1.Columns.Add("SourceTableName", Type.GetType("System.String"));
                        outPutTable1.Columns.Add("SourceParameterName", Type.GetType("System.String"));
                        outPutTable1.Columns.Add("TargetTableName", Type.GetType("System.String"));
                        outPutTable1.Columns.Add("TargetParameterName", Type.GetType("System.String"));
                        outPutTable1.Columns.Add("Value", Type.GetType("System.String"));

                        foreach (DataRow dataRow in outPutTable.Rows)
                        {
                            outPutDataRow = outPutTable1.NewRow();
                            outPutTable1.Rows.Add(outPutDataRow);
                            outPutDataRow["SourceTableName"] = dataRow["SourceTableName"];
                            outPutDataRow["SourceParameterName"] = dataRow["SourceParameterName"];
                            outPutDataRow["TargetTableName"] = dataRow["TargetTableName"];
                            outPutDataRow["TargetParameterName"] = dataRow["TargetParameterName"];
                            outPutDataRow["Value"] = dataRow["Value"].ToString();
                        }

                        response.DataSet.Tables.Add(outPutTable1);
                    }
                }

                response.Status = Status.OK;
            }
            finally
            {
                if (databaseList != null)
                    foreach (Database.IDatabase database1 in databaseList.Values)
                        try
                        {
                            database1.Close();
                        }
                        catch (Exception) { }
            }

            return (Response)response;
        }

        private void CreateAndOpenDatabase(Dictionary<string, Database.IDatabase> databaseList, ServiceDataSet serviceDataSet)
        {
            Database.IDatabase database;
            string databaseName;
            
            //serviceDataSet[i].ConnectionName 으로 database 추가
            for (int i = 0; i < ((IServiceDataSet)serviceDataSet).Count; i++)
            {
                databaseName = ((IServiceDataSet)serviceDataSet)[i].ConnectionName;
                if (!databaseList.ContainsKey(databaseName))
                {
                    database = this.CreateAndOpenDatabase(databaseName, ((IServiceDataSet)serviceDataSet)[i].CommandType);

                    if (database != null)
                        databaseList.Add(databaseName, database);
                }
            }
        }


        private Database.IDatabase CreateAndOpenDatabase(string connectionName, System.Data.CommandType commandType)
        {
            Database.IDatabase database;

            if (connectionName == null)
                return null;

            database = this.databaseAdapter.CreateDatabase(connectionName);
            database.Command.CommandType = commandType;
            //database.Connection.Open();

            return database;
        }

        #region
        [Author("권대선", 2017, 9, 25, AuthorAttributeType.Create, @"
        /// <summary>
        /// DatabaseNames을 가져옵니다.
        /// </summary>
        /// <returns>IResponse.DataSet.Tables[""DatabaseNames""].Rows[index][""DatabaseNames""]</returns>")]
        #endregion
        Response GetDatabaseConnectionNames()
        {
            IResponse response;
            DataTable dataTable;
            DataRow dataRow;
            string[] databaseConnectionNames;

            response = (IResponse)Factory.CreateInstance("Atomus.Service.Response", false, true);

            dataTable = new DataTable("DatabaseNames");

            dataTable.Columns.Add("DatabaseNames", Type.GetType("System.String"));
            dataTable.Columns.Add("Database", Type.GetType("System.String"));

            databaseConnectionNames = this.databaseAdapter.DatabaseConnectionNames;

            foreach (string tmp in databaseConnectionNames)
            {
                dataRow = dataTable.NewRow();
                dataRow["DatabaseNames"] = tmp;
                dataTable.Rows.Add(dataRow);
            }

            response.DataSet.Tables.Add(dataTable);

            return (Response)response;
        }
    }
}
