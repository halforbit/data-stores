using System;

namespace Halforbit.DataStores
{
    public class ValidationException : Exception
    {
        public ValidationException(
            ValidationErrors validationErrors)
        {
            ValidationErrors = validationErrors ?? ValidationErrors.Empty;
        }

        public override string Message => $"Validation error(s) occurred: {ValidationErrors}";

        public ValidationErrors ValidationErrors { get; }
    }
}
