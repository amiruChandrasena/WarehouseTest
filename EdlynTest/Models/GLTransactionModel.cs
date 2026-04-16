using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class GLSettingsModel
    {
        public string Comment { get; set; }
        public string ParamName { get; set; }
        public string ParamValChar { get; set; }
        public double ParamValFloat { get; set; }
        public int ParamValInt { get; set; }
        public string Status { get; set; }
        public string WhenChanged { get; set; }
        public string WhoChanged { get; set; }
    }

    public class GLBatchHeaderModel
    {
        public string Originator { get; set; }
        public string CompanyCode { get; set; }
        public int BatchNo { get; set; }
        public string Description { get; set; }
        public string Source { get; set; }
        public string Status { get; set; }
        public string CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime ModifiedDate { get; set; }
        public double JournalAmount { get; set; }
        public List<GLJnlDetailModel> JnlDetails { get; set; }
    }

    public class GLJnlDetailModel
    {
        public int FinYear { get; set; }
        public int FinPeriod { get; set; }
        public DateTime JnlDate { get; set; }
        public int EntryNo { get; set; }
        public List<GLJnlGLDetailModel> JnlGLDetails { get; set; }
    }

    public class GLJnlGLDetailModel
    {
        public int BatchNo { get; set; }
        public int SortNo { get; set; }
        public int FinYear { get; set; }
        public int FinPeriod { get; set; }
        public int EntryNo { get; set; }
        public string JnlDate { get; set; }
        public string SourceType { get; set; }
        public string SourceDoc { get; set; }
        public string SourceCurrency { get; set; }
        public double SourceDb { get; set; }
        public double SourceCr { get; set; }
        public double SourceAmount { get; set; }
        public double JnlAmount { get; set; }
        public double JnlDB { get; set; }
        public double JnlCR { get; set; }
        public double ExchRate { get; set; }
        public string GLAccount { get; set; }
        public DateTime SourceTransDate { get; set; }
        public double Quantity { get; set; }
        public string Reference { get; set; }
        public string Comment { get; set; }
    }

    public class GLJnlGLSourceModel
    {
        public string Source { get; set; }
        public string SourceDesc { get; set; }
    }

    public class CompanySegmentModel
    {
        public string CompanyCode { get; set; }
        public string SegNo { get; set; }
        public string SegName { get; set; }
        public string SegLength { get; set; }
        public int SegUsed { get; set; }
    }

    public class EoyHistoryModel
    {
        public string CompanyCode { get; set; }
        public int GlBatchNo { get; set; }
        public int Posted { get; set; }
        public int ProcYear { get; set; }
    }

    public class GlAccountMasterModel
    {
        public decimal AccountBalance { get; set; }
        public string AccountGroup { get; set; }
        public string AccountType { get; set; }
        public string AutoAllocate { get; set; }
        public string CompanyCode { get; set; }
        public string Currency { get; set; }
        public string Description { get; set; }
        public string GlAccount { get; set; }
        public string NormalBalance { get; set; }
        public string PostToMethod { get; set; }
        public string Status { get; set; }
        public int VersionControlNo { get; set; }
        public string InterCompLoanAcc { get; set; }
    }

    public class GlAccountBalanceModel
    {
        public int FinYear { get; set; }
        public string GlAccount { get; set; }
        public decimal OpenBalance { get; set; }
        public decimal Period1 { get; set; }
        public decimal Period2 { get; set; }
        public decimal Period3 { get; set; }
        public decimal Period4 { get; set; }
        public decimal Period5 { get; set; }
        public decimal Period6 { get; set; }
        public decimal Period7 { get; set; }
        public decimal Period8 { get; set; }
        public decimal Period9 { get; set; }
        public decimal Period10 { get; set; }
        public decimal Period11 { get; set; }
        public decimal Period12 { get; set; }
        public decimal Period13 { get; set; }
        public decimal Period14 { get; set; }
        public decimal Period15 { get; set; }
        public double QuantityPeriod1 { get; set; }
        public double QuantityPeriod2 { get; set; }
        public double QuantityPeriod3 { get; set; }
        public double QuantityPeriod4 { get; set; }
        public double QuantityPeriod5 { get; set; }
        public double QuantityPeriod6 { get; set; }
        public double QuantityPeriod7 { get; set; }
        public double QuantityPeriod8 { get; set; }
        public double QuantityPeriod9 { get; set; }
        public double QuantityPeriod10 { get; set; }
        public double QuantityPeriod11 { get; set; }
        public double QuantityPeriod12 { get; set; }
        public double QuantityPeriod13 { get; set; }
        public double QuantityPeriod14 { get; set; }
        public double QuantityPeriod15 { get; set; }
        public int VersionControlNo { get; set; }
    }

    public class GlIntCompBal
    {
        public string CompanyCode { get; set; }
        public double Gst { get; set; }
        public double Total { get; set; }
    }
}
