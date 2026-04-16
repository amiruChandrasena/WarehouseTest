using System;
using System.Collections.Generic;
using System.Text;

namespace Models.Dto
{
    public class CycleCountDto
    {
        public string Originator { get; set; }
        public string BinLocation { get; set; }
        public bool IsEmpty { get; set; }
        public List<CycleCountPallet> Pallets { get; set; }
        public List<CycleCountRMPallet> RmPallets { get; set; }
    }
}
