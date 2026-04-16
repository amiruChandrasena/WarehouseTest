using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PalletValidationModel
    {
        public int[] PalletNumbers { get; set; }
        public string ScanData { get; set; }
        public string Originator { get; set; }
        public string WarehouseCode { get; set; }
        public string RoomCode { get; set; }
        public bool IsReplenish { get; set; }
        public bool IsPulldown { get; set; }
    }
}
