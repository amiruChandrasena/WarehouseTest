using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PalletStatusModel
    {
        public int PalletNo { get; set; }
        public bool IsPicked { get; set; }
        public bool IsStatus { get; set; }
        public bool HasShelfLife { get; set; }
        public string Quality { get; set; }
    }
}
