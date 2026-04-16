using System;
using System.Collections.Generic;
using System.Text;

namespace Models.Dto
{
    public class CountPickDto
    {
        public string Originator { get; set; }
        public string BinLocation { get; set; }
        public int PalletNumber { get; set; }
        public string CatalogCode { get; set; }
        public int PalletUnits { get; set; }
        public int UnitsBeforeChange { get; set; }
        public string WarehouseId { get; set; }
        public string Description { get; set; }
        public string Narration { get; set; }
        public int AdjustedQuantity { get; set; }
        public string MovementType { get; set; }
        public string MovementDate { get; set; }
        public List<PalletLabelModel> PalletLabels { get; set; }
    }
}
