// MigratingTable
// Copyright (c) Microsoft Corporation; see license.txt

using Microsoft.PSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migration
{
    public static class PSharpNondeterminism
    {
        public static int Choice(int numChoices)
        {
            int num = 0;
            int denom = 1;
            while (denom < numChoices)
            {
                denom *= 2;
                num = 2 * num + (PSharpRuntime.Nondeterministic() ? 1 : 0);
            }
            // XXX unbalanced
            return (num >= numChoices) ? num - numChoices : num;
        }
    }
}
