using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class WarehouseConfig
    {
        public string Description { get; set; }
        public int FillLevel { get; set; }
        public string LocationCode { get; set; }
        public int MaxCapacity { get; set; }
        public string ProductionArea { get; set; }
        public string Type { get; set; }
        public string WarehouseId { get; set; }
        public string WhRoomCode { get; set; }
        public int SkipValidation { get; set; }
        public string AssignedPersonOne { get; set; }
        public string AssignedPersonTwo { get; set; }
    }
}
