#pragma once

#include "Operator.h"

class FilterOperator : public Operator
{
public:

	// For our limmited example, we simplify the filter to a single integer comparison
	// If the value in the column is greater than FilterValue, we keep it
    FilterOperator(std::unique_ptr<Operator> Child, int FilterValue, ExecutionMode Mode);

    DataChunk Next() override;

private:
    DataChunk ApplyAvx2Filter(const DataChunk& InputChunk);
    DataChunk ApplyScalarFilter(const DataChunk& InputChunk);
	std::unique_ptr<Operator> ChildOperator; // Typically a ScanOperator or MemoryScanOperator
	int ValueToCompare; // If x > ValueToCompare, keep x
};

