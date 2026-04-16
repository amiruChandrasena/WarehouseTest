using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class CheckItModel
    {
        public int ManifestNumber { get; set; }
        public int PicklistNumber { get; set; }
        public string CarrierCode { get; set; }
        public string PalletCount { get; set; }
        public string CustomerName { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
    }
}
