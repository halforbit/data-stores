using Halforbit.ObjectTools.Extensions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Halforbit.DataStores.Validation.Model
{
    public class ValidationErrors : IReadOnlyList<ValidationError>
    {
        internal readonly IReadOnlyList<ValidationError> _validationErrors;

        static ValidationErrors()
        {
            Empty = new ValidationErrors(new ValidationError[] { });
        }

        internal ValidationErrors(IReadOnlyList<ValidationError> validationErrors)
        {
            _validationErrors = validationErrors;
        }

        public static ValidationErrors Empty { get; private set; }

        public int Count => _validationErrors.Count;

        public ValidationError this[int index] => _validationErrors[index];

        public IEnumerator<ValidationError> GetEnumerator() => _validationErrors.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _validationErrors.GetEnumerator();

        public override string ToString() => string.Join("\r\n", _validationErrors);
    }

    public static class ValidationErrorsExtensions
    {
        public static ValidationErrors With(
            this ValidationErrors source,
            ValidationError validationError)
        {
            var validationErrors = new List<ValidationError>(source.Count + 1);

            validationErrors.AddRange(source);

            validationErrors.Add(validationError);

            return new ValidationErrors(validationErrors);
        }

        public static ValidationErrors With(
            this ValidationErrors source,
            IReadOnlyList<ValidationError> validationErrors)
        {
            var newValidationErrors = new List<ValidationError>(source.Count + validationErrors.Count);

            newValidationErrors.AddRange(source);

            newValidationErrors.AddRange(validationErrors);

            return new ValidationErrors(newValidationErrors);
        }

        public static ValidationErrors If(
            this ValidationErrors source,
            bool predicate,
            ValidationError validationError)
        {
            return predicate ?
                source.With(validationError) :
                source;
        }

        public static ValidationErrors Require<TValue>(
            this ValidationErrors source,
            TValue value,
            string name)
        {
            var valueType = typeof(TValue);

            var enumerableInterface = valueType
                .GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IReadOnlyCollection<>));

            var missing = false;

            if (value is string str)
            {
                missing = string.IsNullOrWhiteSpace(str);
            }
            else if (valueType == typeof(Guid?))
            {
                var g = (Guid?)(object)value;

                missing = !g.HasValue || g.Value == Guid.Empty;
            }
            else if (value == null)
            {
                missing = true;
            }
            else if (enumerableInterface != null)
            {
                missing = (int)valueType.GetProperty(nameof(IReadOnlyCollection<object>.Count)).GetValue(value) == 0;
            }
            else
            {
                missing = value.IsDefaultValue();
            }

            return missing ?
                source.With(ValidationError.Required(name)) :
                source;
        }
    }
}
