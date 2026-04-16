using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class LoadingPallet
    {
        public int PalletNumber { get; set; }
        public int InvoiceNumber { get; set; }
        public string CustomerCode { get; set; }
        public int AssigneeNumber { get; set; }
        public int PicklistNumber { get; set; }
        public int LoadingConfirmed { get; set; }
        public string CarrierName { get; set; }
    }
}
