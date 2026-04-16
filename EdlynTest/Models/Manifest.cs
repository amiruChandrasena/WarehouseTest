using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class Manifest
    {
        public int AdviceFlag { get; set; }
        public string AreaCode { get; set; }
        public string CarrierCode { get; set; }
        public int ConfirmFlag { get; set; }
        public DateTime DateCreated { get; set; }
        public int LoadOption { get; set; }
        public string LoadingStatus { get; set; }
        public int ManifestNumber { get; set; }
        public double NumberOfPallets { get; set; }
        public int OpenPalletNumber { get; set; }
        public string PalletType { get; set; }
        public double PalletWeight { get; set; }
        public int PlanFlag { get; set; }
        public string Rego { get; set; }
        public string Remark { get; set; }
        public int ReserveFlag { get; set; }
        public DateTime RunDate { get; set; }
        public int RunNumber { get; set; }
        public int Select { get; set; }
        public string Status { get; set; }
        public DateTime TimeIn { get; set; }
        public DateTime TimeOut { get; set; }
        public int TruckFront { get; set; }
        public int TruckRear { get; set; }
        public int Version { get; set; }
    }
}
