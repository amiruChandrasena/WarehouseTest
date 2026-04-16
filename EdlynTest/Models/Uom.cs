using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class Uom
    {
        public double Conversion { get; set; }
        public string UnitOfMeasure{ get; set; }
        public string Description { get; set; }
        public int Version { get; set; }
    }
}
