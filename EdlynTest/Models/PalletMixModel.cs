using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PalletMixModel
    {
        public int OldPalletNumber { get; set; }
        public string Originator { get; set; }
        public string WarehouseCode { get; set; }
        public string RackingZone { get;}
        public string BinLocationTo { get; set; }
        public PalletDetail palletDetail { get; set; }
    }
}
