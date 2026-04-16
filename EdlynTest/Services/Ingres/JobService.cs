using Abstractions.ServiceInterfaces;
using Common;
using Microsoft.Extensions.Configuration;
using Models;
using Models.Utility;
using Services.Ingres.SQLResources;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;

namespace Services.Ingres
{
    public class JobService : IJobService
    {
        private readonly string connectionString;

        public JobService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper GetRMJobDetailsByJobNo(int jobNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            JobHeaderRMModel header = new JobHeaderRMModel();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = JobSQL.ResourceManager.GetString("GetRMJobDetailsByJobNo");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@JobNo", OdbcType.VarChar).Value = jobNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    header = new JobHeaderRMModel
                                    {
                                        JobNo = dReader.job_no,
                                        Allergens = dReader.allergens,
                                        AvailTime = dReader.avail_time,
                                        BomVertion = dReader.bom_version,
                                        CatalogCode = dReader.catlog_code,
                                        Ccp = dReader.ccp,
                                        CcpStatus = dReader.ccp_status,
                                        EndTime = dReader.end_time,
                                        JobSeq = dReader.job_seq,
                                        LabelTime = dReader.label_time,
                                        PackTime = dReader.pack_time,
                                        PlanQty = dReader.plan_qty,
                                        PlanUom = dReader.plan_uom,
                                        SavedBy = dReader.saved_by,
                                        SavedOn = dReader.saved_on,
                                        ScheduleNo = dReader.schedule_no,
                                        ScheduleSeq = dReader.schedule_seq,
                                        SellingCode = dReader.selling_code,
                                        ShelfLife = dReader.shelflife,
                                        StartTime = dReader.start_time,
                                        Status = dReader.status,
                                        UOM = dReader.uom,
                                        CatalogDesc = dReader.description,
                                        ByProductCode = dReader.by_product_code,
                                        ByProductQty = dReader.by_product_qty,
                                        JobType = dReader.job_type,
                                        ReWorkNo = dReader.rework_no,
                                        LineNo = dReader.line_no,
                                        LineDescription = dReader.description,
                                        ActualQtyUnit = dReader.actual_qty,
                                        ActualQty = dReader.actual_qty,
                                        PackDate = dReader.pack_date,
                                        EndDate = dReader.finish_date
                                    };
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetRMJobDetailsByJobNo(int): No job details found for job no " + jobNo.ToString());
                                return wrapper;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetRMJobDetailsByJobNo(int): " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(header);
            return wrapper;
        }

        public string GetRMReworkInstructionByJobNo(int jobNo)
        {
            string rWorkInstruction = "";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = JobSQL.ResourceManager.GetString("GetRMReworkInstructionByJobNo");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@JobNo", OdbcType.VarChar).Value = jobNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    rWorkInstruction = dReader.instructions;
                                }
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    rWorkInstruction = "";
                }
            }

            return rWorkInstruction;
        }

        public TransactionWrapper GetAllJobDetailsByJobNo(int jobNo, string CatalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<JobDetailsRMModel> transDetails = new List<JobDetailsRMModel>();

            //For Raw Materials
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = JobSQL.ResourceManager.GetString("GetAllRawMaterialJobDetailsByJobNo");

                    queryString = queryString + " WHERE j.rm_number = r.rm_number AND j.rm_number = ca.catlog_code ";

                    if (jobNo > 0)
                        queryString = queryString + " AND j.job_no = " + jobNo.ToString();

                    if (!string.IsNullOrEmpty(CatalogCode))
                        queryString = queryString + " AND j.rm_number =  '" + CatalogCode + "' ";

                    queryString = queryString + " ORDER BY cost_item_no ASC";

                    //queryString = "SELECT * FROM (" + queryString + ") a ORDER BY cost_id ASC, uom ASC";

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    JobDetailsRMModel detail = new JobDetailsRMModel
                                    {
                                        JobNo = dReader.job_no,
                                        ReqQty = Math.Round(dReader.req_qty, Common.Common.decimalPlaces),
                                        CatalogCode = dReader.rm_number.Trim(),
                                        RMType = dReader.rm_type.Trim(),
                                        CostItemNo = dReader.cost_item_no,
                                        UsedQty = Math.Round(dReader.used_qty, Common.Common.decimalPlaces),
                                        Uom = dReader.uom,
                                        RateTonne = Math.Round(dReader.rate_tonne, Common.Common.decimalPlaces),
                                        RateUom = Math.Round(dReader.rate_tonne, Common.Common.decimalPlaces),
                                        MixBy = dReader.mix_by,
                                        WarehouseId = dReader.warehouse_id.Trim(),
                                        Status = dReader.status.Trim(),
                                        Description = dReader.description.Trim()
                                    };

                                    if (detail.Status == "B")
                                        detail.StatusDescription = "Batched";
                                    else if (detail.Status == "I")
                                        detail.StatusDescription = "Issued";
                                    else if (detail.Status == "U")
                                        detail.StatusDescription = "Used";
                                    else
                                        detail.StatusDescription = detail.Status;

                                    transDetails.Add(detail);
                                }
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetAllRawMaterialJobDetailsByJobNo: " + e.Message);
                    return wrapper;
                }
            }

            //For Other Job Cards
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = JobSQL.ResourceManager.GetString("GetAllOtherJobDetailsByJobNo");

                    queryString = queryString + " AND j.job_no = " + jobNo.ToString();

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    JobDetailsRMModel detail = new JobDetailsRMModel
                                    {
                                        JobNo = dReader.job_no,
                                        ReqQty = dReader.req_qty,
                                        CatalogCode = dReader.rm_number.Trim(),
                                        RMType = dReader.rm_type.Trim(),
                                        CostItemNo = dReader.cost_item_no,
                                        LotId = dReader.lot_id,
                                        UsedQty = dReader.used_qty,
                                        Uom = dReader.plan_uom,
                                        //RateTonne = dReader.rate_tonne,
                                        RateUom = dReader.rate_uom,
                                        MixBy = dReader.mix_by,
                                        //WarehouseId = dReader.warehouse_id.Trim(),
                                        Status = dReader.status.Trim(),
                                        Description = dReader.description.Trim()
                                    };

                                    if (detail.Status == "B")
                                        detail.StatusDescription = "Batched";
                                    else if (detail.Status == "I")
                                        detail.StatusDescription = "Issued";
                                    else if (detail.Status == "U")
                                        detail.StatusDescription = "Used";
                                    else
                                        detail.StatusDescription = detail.Status;

                                    //get the rate per uom for the job 
                                    detail.RateUom = GetRatePerUomByJobNo(jobNo);

                                    transDetails.Add(detail);
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetAllOtherJobDetailsByJobNo: " + e.Message);
                    return wrapper;
                }
            }

            //FINISHED GOODS 
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = JobSQL.ResourceManager.GetString("GetAllFinishedGoodsDetailsByJobNo");

                    queryString = queryString + " AND j.job_no = " + jobNo.ToString();

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    JobDetailsRMModel detail = new JobDetailsRMModel
                                    {
                                        JobNo = dReader.job_no,
                                        ReqQty = dReader.req_qty,
                                        CatalogCode = dReader.rm_number.Trim(),
                                        RMType = dReader.rm_type.Trim(),
                                        CostItemNo = dReader.cost_item_no,
                                        LotId = dReader.lot_id,
                                        UsedQty = dReader.used_qty,
                                        Uom = dReader.uom_stock,
                                        //RateTonne = dReader.rate_tonne,
                                        RateUom = dReader.rate_uom,
                                        MixBy = dReader.mix_by,
                                        //WarehouseId = dReader.warehouse_id.Trim(),
                                        Status = dReader.status.Trim(),
                                        Description = dReader.description.Trim()
                                    };

                                    if (detail.Status == "B")
                                        detail.StatusDescription = "Batched";
                                    else if (detail.Status == "I")
                                        detail.StatusDescription = "Issued";
                                    else if (detail.Status == "U")
                                        detail.StatusDescription = "Used";
                                    else
                                        detail.StatusDescription = detail.Status;

                                    //get the rate per uom for the job 
                                    detail.RateUom = GetRatePerUomByJobNo(jobNo);

                                    transDetails.Add(detail);
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetAllFinishedGoodsDetailsByJobNo: " + e.Message);
                    return wrapper;
                }
            }

            //Have to Sort by Cost Item No
            if (transDetails.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetAllJobDetailsByJobNo(int,string): No job details found");
                return wrapper;
            }
            else
            {
                wrapper.IsSuccess = true;
                wrapper.ResultSet.Add(transDetails);
                return wrapper;
            }

        }

        public float GetRatePerUomByJobNo(int jobNo)
        {
            float units = 0;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = JobSQL.ResourceManager.GetString("GetRatePerUomByJobNo");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@JobNo", OdbcType.Int).Value = jobNo;
                        //command.Parameters.Add("@RoomTypeLike", OdbcType.VarChar).Value = roomTypeLike;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    units = dReader.pallet_no;
                                }
                            }
                        }

                        return units;
                    }
                }
                catch (Exception e)
                {
                    return units;
                }
            }
        }

        public TransactionWrapper GetAllJobsSearchList(int jobNo, string lineNo, string catalogCode, DateTime fromDate, DateTime toDate, string status, int glUpdate)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<JobHeaderRMModel> headerList = new List<JobHeaderRMModel>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = JobSQL.ResourceManager.GetString("GetAllJobsSearchList");

                    queryString = queryString + " WHERE jh.catlog_code = ca.catlog_code AND " +
                        "ca.uom_stock = u.uom AND " +
                        "jh.job_no = wip.so_no ";

                    if (jobNo > 0)
                        queryString = queryString + " AND jh.Job_no = '" + jobNo + "'";

                    if (!string.IsNullOrEmpty(lineNo))
                        queryString = queryString + " AND wip.Line_no = '" + lineNo + "'";

                    if (!string.IsNullOrEmpty(catalogCode))
                        queryString = queryString + " AND jh.catlog_code = '" + catalogCode + "'";

                    if (!string.IsNullOrEmpty(catalogCode))
                        queryString = queryString + " AND jh.catlog_code = '" + catalogCode + "'";

                    if (fromDate != DateTime.MinValue)
                        queryString = queryString + " AND jh.start_time >= '" + fromDate.Date + " 00:00:01'";

                    if (toDate != DateTime.MinValue)
                        queryString = queryString + " AND jh.start_time <= '" + toDate.Date + " 23:59:59'";

                    if (!string.IsNullOrEmpty(status))
                        queryString = queryString + " AND jh.status IN (" + status + ")";

                    if (glUpdate > 0)
                        queryString = queryString + " AND jh.gl_update = " + glUpdate;

                    queryString = queryString + " ORDER BY jh.start_time ASC ";

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    JobHeaderRMModel header = new JobHeaderRMModel
                                    {
                                        JobNo = dReader.job_no,
                                        CatalogCode = dReader.catlog_code,
                                        JobSeq = dReader.job_seq,
                                        PlanQty = dReader.plan_qty,
                                        PlanQtyUnit = dReader.batch_qty,
                                        SavedBy = dReader.saved_by,
                                        SavedOn = dReader.saved_on,
                                        ScheduleNo = dReader.schedule_no,
                                        ScheduleSeq = dReader.schedule_seq,
                                        StartTime = dReader.start_time,
                                        Status = dReader.status,
                                        CatalogDesc = dReader.description,
                                        LineNo = dReader.line_no,
                                        ActualQtyUnit = dReader.actual_qty,
                                        //ActualQty = dReader.actual_qty * dReader.conversion * 1000,
                                        ActualQty = reader.GetDouble(1),
                                        GLUpdate = dReader.gl_update
                                    };

                                    header.LineDescription = GetLineDescriptionByLineNo(header.LineNo);
                                    header.UOM = GetUomByCatalogCode(header.CatalogCode);
                                    header.Yeild = (header.ActualQty / header.PlanQty) * 100;

                                    JobHeaderRMModel headerBaseUOM = GetBaseUOMByCatalogCode(header.CatalogCode);
                                    header.BaseUOM = headerBaseUOM.BaseUOM;
                                    header.BaseCode = headerBaseUOM.BaseCode;

                                    header.Conversion = GetUomConversionByBaseUom(header.BaseUOM);
                                    header.FinishedGoodsQty = (header.ActualQty / 1000) * header.Conversion;

                                    headerList.Add(header);
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetAllJobsSearchList: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(headerList);
            return wrapper;
        }

        public string GetLineDescriptionByLineNo(string lineNo)
        {
            string lineDesc = "";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = JobSQL.ResourceManager.GetString("GetLineDescriptionByLineNo");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@LineNo", OdbcType.VarChar).Value = lineNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    lineDesc = dReader.instructions;
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    lineDesc = "";
                }
            }

            return lineDesc;
        }

        public string GetUomByCatalogCode(string catalogCode)
        {
            string uom = "";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = JobSQL.ResourceManager.GetString("GetUomByCatalogCode");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    uom = dReader.instructions;
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    uom = "";
                }
            }

            return uom;
        }

        public JobHeaderRMModel GetBaseUOMByCatalogCode(string catalogCode)
        {
            JobHeaderRMModel header = new JobHeaderRMModel();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = JobSQL.ResourceManager.GetString("GetBaseUOMByCatalogCode");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    header = new JobHeaderRMModel
                                    {
                                        BaseUOM = dReader.uom_stock,
                                        BaseCode = dReader.base_code
                                    };
                                }
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    header = new JobHeaderRMModel();
                }
            }

            return header;
        }

        public TransactionWrapper GetJobDetailByTagId(string tagId)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            JobDetailsRMModel detail = new JobDetailsRMModel();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = JobSQL.ResourceManager.GetString("GetJobDetailByTagId");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@TagId", OdbcType.VarChar).Value = tagId;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    detail = new JobDetailsRMModel
                                    {
                                        JobNo = dReader.uom_stock,
                                        CatalogCode = dReader.base_code,
                                        CostItemNo = dReader.base_code
                                    };
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetJobDetailByTagId: No job details found for tag ID " + tagId);
                                return wrapper;
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetJobDetailByTagId: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(detail);
            return wrapper;
        }

        public float GetUomConversionByBaseUom(string baseUom)
        {
            float conversion = 0;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = JobSQL.ResourceManager.GetString("GetUomConversionByBaseUom");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BaseUom", OdbcType.Int).Value = baseUom;
                        //command.Parameters.Add("@RoomTypeLike", OdbcType.VarChar).Value = roomTypeLike;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    conversion = dReader.pallet_no;
                                }
                            }
                        }

                        return conversion;
                    }
                }
                catch (Exception e)
                {
                    return conversion;
                }
            }
        }

        public TransactionWrapper GetCostPeriodByDate(DateTime date)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            JobCostPeriodModel period = new JobCostPeriodModel();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = JobSQL.ResourceManager.GetString("GetCostPeriodByDate");

                    queryString = queryString + " WHERE '" + date.ToString(DateFormats.MMddyy) + "' BETWEEN date_from AND date_to ";

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        //command.Parameters.Add("@Date", OdbcType.VarChar).Value = date;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    period = new JobCostPeriodModel
                                    {
                                        CostYear = dReader.cost_year,
                                        CostPeriod = dReader.cost_period
                                    };
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetCostPeriodByDate: No cost period found");
                                return wrapper;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetCostPeriodByDate: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(period);
            return wrapper;
        }

        public TransactionWrapper GetAllJobDetailsByJobNo(int jobNo, int CostItemNo, string CatalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<JobDetailsRMModel> transDetails = new List<JobDetailsRMModel>();

            //For Raw Materials
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = JobSQL.ResourceManager.GetString("GetAllJobDetailsByJobNo");

                    queryString = queryString + " WHERE ";

                    if (jobNo > 0)
                        queryString = queryString + "j.job_no = " + jobNo.ToString(); // job number has to be present, method makes no sense otherwise

                    if (CostItemNo > 0)
                        queryString = queryString + " AND j.cost_item_no = " + CostItemNo.ToString();

                    if (!string.IsNullOrEmpty(CatalogCode))
                        queryString = queryString + " AND j.rm_number = " + "'" + CatalogCode + "'";

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    JobDetailsRMModel detail = new JobDetailsRMModel
                                    {
                                        JobNo = dReader.job_no,
                                        ReqQty = dReader.req_qty,
                                        CatalogCode = dReader.rm_number.Trim(),
                                        RMType = dReader.rm_type.Trim(),
                                        CostItemNo = dReader.cost_item_no,
                                        UsedQty = dReader.used_qty
                                    };

                                    transDetails.Add(detail);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetAllJobDetailsByJobNo(int,int,string): No job details found");
                                return wrapper;
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetAllJobDetailsByJobNo(int,int,string): " + e.Message);
                    return wrapper;
                }
            }

            //Have to Sort by Cost Item No
            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(transDetails);
            return wrapper;
        }

        public TransactionWrapper GetRMJobDetailsByJobNo(string jobNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            JobHeaderRMModel header = new JobHeaderRMModel();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = IssueSQL.ResourceManager.GetString("GetRMJobDetailsByJobNo");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@JobNo", OdbcType.VarChar).Value = jobNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    header = new JobHeaderRMModel
                                    {
                                        JobNo = dReader.job_no,
                                        Allergens = dReader.allergens,
                                        AvailTime = dReader.avail_time,
                                        BomVertion = dReader.bom_version,
                                        CatalogCode = dReader.catlog_code,
                                        Ccp = dReader.ccp,
                                        CcpStatus = dReader.ccp_status,
                                        EndTime = dReader.end_time,
                                        JobSeq = dReader.job_seq,
                                        LabelTime = dReader.label_time,
                                        PackTime = dReader.pack_time,
                                        PlanQty = dReader.plan_qty,
                                        PlanUom = dReader.plan_uom,
                                        SavedBy = dReader.saved_by,
                                        SavedOn = dReader.saved_on,
                                        ScheduleNo = dReader.schedule_no,
                                        ScheduleSeq = dReader.schedule_seq,
                                        SellingCode = dReader.selling_code,
                                        ShelfLife = dReader.shelflife,
                                        StartTime = dReader.start_time,
                                        Status = dReader.status,
                                        UOM = dReader.uom,
                                        CatalogDesc = dReader.description,
                                        ByProductCode = dReader.by_product_code,
                                        ByProductQty = dReader.by_product_qty,
                                        JobType = dReader.job_type,
                                        ReWorkNo = dReader.rework_no,
                                        LineNo = dReader.line_no,
                                        LineDescription = dReader.description,
                                        ActualQtyUnit = dReader.actual_qty,
                                        ActualQty = dReader.actual_qty,
                                        PackDate = dReader.pack_date,
                                        EndDate = dReader.finish_date
                                    };
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetRMJobDetailsByJobNo(string): No job details found for Job No " + jobNo);
                                return wrapper;
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetRMJobDetailsByJobNo(string): " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(header);
            return wrapper;
        }

        public TransactionWrapper GetRMRoomConfigByCatalogCode(string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<RoomConfigModel> headerList = new List<RoomConfigModel>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = IssueSQL.ResourceManager.GetString("GetRMRoomConfigByCatalogCode");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    RoomConfigModel header = new RoomConfigModel
                                    {
                                        WarehouseCode = dReader.job_no,
                                        RoomCode = dReader.allergens,
                                        RackCode = dReader.avail_time
                                    };

                                    headerList.Add(header);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetRMRoomConfigByCatalogCode: No room configs found for catalog code " + catalogCode);
                                return wrapper;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetRMRoomConfigByCatalogCode: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(headerList);
            return wrapper;
        }

        public TransactionWrapper SaveJobMix(JobMixHeader mixHeader)
        {
            int maxMixNo = 0;
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = JobSQL.ResourceManager.GetString("UpdateNewJobMixNo");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@KeyType", OdbcType.VarChar).Value = "MXNO";
                        int rowsAffected = command.ExecuteNonQuery();
                    }

                    string queryString = JobSQL.ResourceManager.GetString("GetMaxJobMixNo");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@KeyType", OdbcType.VarChar).Value = "MXNO";
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {

                            while (reader.Read())
                            {
                                maxMixNo = reader.GetInt32(0);
                            }
                        }
                    }

                    //maxMixNo = maxMixNo + 1;

                    mixHeader.MixNo = maxMixNo;

                    string insertJobMixHeaderString = IssueSQL.ResourceManager.GetString("InsertJobMixHeader");
                    using (OdbcCommand command = new OdbcCommand(insertJobMixHeaderString, connection))
                    {
                        command.Parameters.Add("@form_code", OdbcType.VarChar).Value = mixHeader.CatalogCode;
                        command.Parameters.Add("@job_no", OdbcType.Int).Value = mixHeader.JobNo;
                        command.Parameters.Add("@mix_date", OdbcType.DateTime).Value = mixHeader.MixDate;
                        command.Parameters.Add("@mix_no", OdbcType.Int).Value = mixHeader.MixNo;
                        command.Parameters.Add("@originator", OdbcType.VarChar).Value = mixHeader.Originator;
                        command.Parameters.Add("@phase_code", OdbcType.VarChar).Value = "";
                        command.Parameters.Add("@qty", OdbcType.Double).Value = 0;
                        command.Parameters.Add("@splits", OdbcType.Int).Value = 0;
                        command.Parameters.Add("@type", OdbcType.VarChar).Value = mixHeader.Type;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                    }

                    foreach (JobMixDetail obj in mixHeader.JobMixDetailList)
                    {
                        if (obj.SplitNo == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SaveJobMix : No split number provided, cannot save");
                            return wrapper;
                        }

                        if (obj.CatalogCode == "")
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SaveJobMix : No RM code provided, cannot save");
                            return wrapper;
                        }

                        obj.MixNo = maxMixNo;

                        string insertJobMixDetailsString = IssueSQL.ResourceManager.GetString("InsertJobMixDetails");
                        using (OdbcCommand command = new OdbcCommand(insertJobMixDetailsString, connection))
                        {
                            command.Parameters.Add("@cost_item_no", OdbcType.VarChar).Value = 0;
                            command.Parameters.Add("@job_no", OdbcType.Int).Value = obj.JobNo;
                            command.Parameters.Add("@lot_id", OdbcType.Int).Value = obj.LotId;
                            command.Parameters.Add("@mix_no", OdbcType.Int).Value = obj.MixNo;
                            command.Parameters.Add("@mix_target", OdbcType.Double).Value = obj.ReqQty;
                            command.Parameters.Add("@mix_weight", OdbcType.Double).Value = obj.IssueQty;
                            command.Parameters.Add("@pack_qty_used", OdbcType.Double).Value = 0;
                            command.Parameters.Add("@rm_number", OdbcType.VarChar).Value = obj.CatalogCode;
                            command.Parameters.Add("@split_no", OdbcType.Int).Value = obj.SplitNo;

                            int rowsAffected = command.ExecuteNonQuery();
                            wrapper.IsSuccess = true;
                        }
                    }

                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("SaveJobMix : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePFJobMixDetail(JobMixHeader mixHeader)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = IssueSQL.ResourceManager.GetString("UpdatePFJobMixDetail");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@MixNo", OdbcType.VarChar).Value = mixHeader.MixNo;

                        int rowsAffected = command.ExecuteNonQuery();
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePFJobMixDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper AddUsedQty(int jobNo, string catalogCode, int costItemNo, double qty)
        {
            string queryString = "";
            int rowsAffected = 0;

            string logFileName = String.Format("JobIssueSave_Service_{0}_{1}.txt", DateTime.Now.ToString("yyyyMMdd"), jobNo.ToString());

            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    Common.WriteLogFile.WriteLog(logFileName, String.Format("{0} - {1}", DateTime.Now.ToString(), "UpdateJobDetailsUsedQtyByJobNoCostItemNo " + jobNo.ToString() + " / " + catalogCode + " / " + costItemNo.ToString() + " / No of rows effected : " + rowsAffected.ToString()));

                    List<JobDetailsRMModel> lstJobDetails = GetAllJobDetailsByJobNoCatCode(jobNo, catalogCode);
                    if (lstJobDetails.Count == 0)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Item code " + catalogCode + " is not in job number " + jobNo);
                        return wrapper;
                    }

                    Common.WriteLogFile.WriteLog(logFileName, String.Format("{0} - {1}", DateTime.Now.ToString(), "GetAllJobDetailsByJobNoCatCode " + jobNo.ToString() + " / " + catalogCode + " / " + costItemNo.ToString()));

                    //--IF not cost number has been supplied and more than one instance of the rm numebr 
                    if (costItemNo == 0 && lstJobDetails.Count > 0)
                    {
                        List<JobDetailsRMModel> jobDetList = lstJobDetails.OrderBy(o => o.CostItemNo).ToList();
                        if (costItemNo == 0)
                        {
                            costItemNo = jobDetList[0].CostItemNo;
                        }
                    }

                    if (costItemNo != 0)
                    {
                        queryString = JobSQL.ResourceManager.GetString("UpdateJobDetailsUsedQtyByJobNoCostItemNo");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@IssueQty", OdbcType.Double).Value = qty;
                            command.Parameters.Add("@JobNo", OdbcType.Int).Value = jobNo;
                            command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                            command.Parameters.Add("@CostItemNo", OdbcType.Int).Value = costItemNo;

                            rowsAffected = command.ExecuteNonQuery();
                        }

                        Common.WriteLogFile.WriteLog(logFileName, String.Format("{0} - {1}", DateTime.Now.ToString(), "UpdateJobDetailsUsedQtyByJobNoCostItemNo " + jobNo.ToString() + " / " + catalogCode + " / " + costItemNo.ToString() + " / No of rows effected : " + rowsAffected.ToString()));
                    }
                    else
                    {
                        queryString = JobSQL.ResourceManager.GetString("UpdateJobDetailsUsedQtyByJobNo");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@IssueQty", OdbcType.Double).Value = qty;
                            command.Parameters.Add("@JobNo", OdbcType.Int).Value = jobNo;
                            command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                            rowsAffected = command.ExecuteNonQuery();
                        }

                        Common.WriteLogFile.WriteLog(logFileName, String.Format("{0} - {1}", DateTime.Now.ToString(), "UpdateJobDetailsUsedQtyByJobNo " + jobNo.ToString() + " / " + catalogCode + " / " + costItemNo.ToString() + " / No of rows effected : " + rowsAffected.ToString()));
                    }

                    if (rowsAffected > 0)
                    {
                        wrapper.IsSuccess = true;
                    }
                    else
                    {
                        wrapper.Messages.Add("UpdateJobDetails: Could not find details for selected job and product");
                    }

                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("AddUsedQty : " + e.Message);

                    Common.WriteLogFile.WriteLog(logFileName, String.Format("{0} - {1}", DateTime.Now.ToString(), "Error from " + jobNo.ToString() + " / " + catalogCode + " / " + costItemNo.ToString() + " / Error : " + e.Message));

                    return wrapper;
                }
            }
        }

        public List<JobDetailsRMModel> GetAllJobDetailsByJobNoCatCode(int jobNo, string catalogCode)
        {
            List<JobDetailsRMModel> transDetails = new List<JobDetailsRMModel>();

            //For Raw Materials
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = JobSQL.ResourceManager.GetString("GetAllJobDetailsByJobNoCatCode");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@JobNo", OdbcType.Int).Value = jobNo;
                        command.Parameters.Add("@RmNumber", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    JobDetailsRMModel detail = new JobDetailsRMModel
                                    {
                                        JobNo = dReader.job_no,
                                        ReqQty = dReader.req_qty,
                                        CatalogCode = dReader.rm_number.Trim(),
                                        RMType = dReader.rm_type.Trim(),
                                        CostItemNo = dReader.cost_item_no,
                                        UsedQty = dReader.used_qty,
                                        Status = dReader.status.Trim()
                                    };

                                    if (detail.Status == "B")
                                        detail.StatusDescription = "Batched";
                                    else if (detail.Status == "I")
                                        detail.StatusDescription = "Issued";
                                    else if (detail.Status == "U")
                                        detail.StatusDescription = "Used";
                                    else
                                        detail.StatusDescription = detail.Status;

                                    transDetails.Add(detail);
                                }
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    transDetails = new List<JobDetailsRMModel>();
                }
            }

            //Have to Sort by Cost Item No
            return transDetails;
        }

        public TransactionWrapper UpdateJobHeaderStatus(int jobNo, string status)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = JobSQL.ResourceManager.GetString("UpdateJobHeaderStatus");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@JobNo", OdbcType.Int).Value = jobNo;

                        int rowsAffected = command.ExecuteNonQuery();
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateJobHeaderStatus : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SaveJobOrderLog(JobOrderLogModel jobLog)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = JobSQL.ResourceManager.GetString("SaveJobOrderLog");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@Action", OdbcType.VarChar).Value = jobLog.Action;
                        command.Parameters.Add("@ActionDate", OdbcType.DateTime).Value = jobLog.ActionDate;
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = jobLog.BatchNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = jobLog.CatalogCode;
                        command.Parameters.Add("@CostItemNo", OdbcType.Int).Value = jobLog.CostItemNo;
                        command.Parameters.Add("@ItemType", OdbcType.VarChar).Value = jobLog.ItemType;
                        command.Parameters.Add("@JobNo", OdbcType.Int).Value = jobLog.JobNo;
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = jobLog.Originator;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                    }

                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("SaveJobOrderLog : " + e.Message);
                    return wrapper;
                }
            }
        }
    }
}
